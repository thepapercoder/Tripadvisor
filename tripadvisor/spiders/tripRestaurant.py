# -*- coding: utf-8 -*-
import scrapy
import os
import codecs
import logging
import re
from pprint import pprint
from tqdm import tqdm
import urllib


class TriprestaurantSpider(scrapy.Spider):
    name = 'tripRestaurant'

    custom_settings = {
        'ITEM_PIPELINES': {
            'tripadvisor.pipelines.DuplicatesPipeline': 200,
            'tripadvisor.pipelines.TripRestaurantPipeline': 300,
        },
        "LOG_LEVEL": 'DEBUG',
        "LOG_FILE": './log_res.txt',
        "LOG_ENCODING": 'utf-8',
        # "JOBDIR": 'jobs/tripRestaurant-5'
    }

    html_save_path = "D:\\Working\\Crawling_Tripvisor_Restaurant"

    def __init__(self):
        super(TriprestaurantSpider, self).__init__()
        self.url_regex = re.compile(
            '(https:\/\/www\.tripadvisor\.co\.nz\/Restaurant_Review-g\d+-d\d+-Reviews)-([\w+\-]+\.html)')
        self.user_id_regex = re.compile('UID_(\w+)-SRC_(\w+)')
        base_html_directory = os.path.join(self.html_save_path, "html_saved_res_test")
        if not os.path.isdir(base_html_directory):
            os.mkdir(base_html_directory)
        self.base_html_directory = base_html_directory

    def start_requests(self):
        url = 'https://www.tripadvisor.co.nz/Restaurants-g293921-Vietnam.html/'
        yield scrapy.Request(url=url, callback=self.restaurant_location_list_first_page_parse,
                             meta={'first_page': True})

    def restaurant_location_list_first_page_parse(self, response):
        '''
        #1
        The sample url is: https://www.tripadvisor.co.nz/Restaurants-g293921-oa40-Vietnam.html#LOCATION_LIST
        :param response: response from previous request
        :return:
        '''
        is_first_page = response.meta['first_page']
        if is_first_page:
            # First page have different format
            restaurant_loc_list = response.xpath("//div[@class='geo_name']/a/@href").extract()
            for restaurant in restaurant_loc_list:
                yield response.follow(restaurant, self.restaurant_list_parse, meta={'loop': True})
        else:
            restaurant_loc_list = response.xpath("//ul[@class='geoList']/li/a/@href").extract()
            for restaurant_loc_url in restaurant_loc_list:
                yield response.follow(restaurant_loc_url, self.restaurant_list_parse, meta={'loop': True})
        # General page (paging included)
        base_url = 'https://www.tripadvisor.co.nz/Restaurants-g293921-oa{}-Vietnam.html#LOCATION_LIST'
        self._save_html(response, response.url)
        for i in range(20, 101, 10):
            url = base_url.format(str(i))
            yield scrapy.Request(url=url, callback=self.restaurant_location_list_first_page_parse, meta={'first_page': False})

    def restaurant_list_parse(self, response):
        '''
        #2
        The sample url is: https://www.tripadvisor.co.nz/Restaurants-g293925-Ho_Chi_Minh_City.html
        :param response:
        :return:
        '''
        restaurant_list = response.xpath("//div[contains(@class,'listing rebrand')]//*[@class='title']/a")
        geo_code = self.extract_geo_code(response.url)
        for restaurant in restaurant_list:
            url = restaurant.xpath('./@href').extract_first()
            if url:
                yield response.follow(url, self.extract_restaurant_info, meta={'geo_code': geo_code})
            else:
                logging.log(logging.ERROR, "[01] Can't get restaurant link in " + response.url)
        if response.meta['loop']:
            last_page_number = response.xpath("//div[@class='pageNumbers']/a[last()]/text()").extract_first()
            if last_page_number:
                last_index = int(last_page_number) * 30 + 1
                url = "https://www.tripadvisor.co.nz/RestaurantSearch?Action=PAGE&geo={0}&ajax=1&sortOrder=relevance&o=a{1}&availSearchEnabled=false"
                for i in range(0, last_index, 30):
                    yield scrapy.Request(url=url.format(geo_code, i), callback=self.restaurant_list_parse, meta={'loop': False})

    def extract_restaurant_info(self, response):
        self._save_html(response, response.url)
        res_name = " ".join(response.xpath("//h1[@id='HEADING']/text()").extract()).strip()
        yield {
            'type': 'res_info',
            'url': response.url,
            'geo_code': response.meta['geo_code'],
            "res_name": res_name,
            "ranking": " ".join(response.xpath("//span[@class='header_popularity popIndexValidation']//text()").extract()).strip(),
            "cuisines": " ".join(response.xpath("(//div[@id='RESTAURANT_DETAILS']//div[@class='row'])[contains(./div/text(), 'Cuisine')]/div[@class='content']//text()").extract()).strip(),
            "price": " ".join(response.xpath("(//div[@id='RESTAURANT_DETAILS']//div[@class='row'])[contains(./div/text(), 'price')]/div[@class='content']//text()").extract()).strip(),
            "meals": " ".join(response.xpath("(//div[@id='RESTAURANT_DETAILS']//div[@class='row'])[contains(./div/text(), 'eal')]/div[@class='content']//text()").extract()).strip(),
            "res_feature": " ".join(response.xpath("(//div[@id='RESTAURANT_DETAILS']//div[@class='row'])[contains(./div/text(), 'feature')]/div[@class='content']//text()").extract()).strip(),
            "good_for": " ".join(response.xpath("(//div[@id='RESTAURANT_DETAILS']//div[@class='row'])[contains(./div/text(), 'Good for')]/div[@class='content']//text()").extract()).strip(),
            "open_hours": " ".join(response.xpath("(//div[@id='RESTAURANT_DETAILS']//div[@class='row'])[contains(./div/text(), 'Open Hours')]/div[contains(@class,'content')]//text()").extract()).strip(),
            "food_rating": " ".join(response.xpath("//div[@id='RESTAURANT_DETAILS']//div[contains(@class, 'ratingSummary')]//div[contains(@class, 'ratingRow wrap') and ./div[contains(@class, 'label') and contains(./span/text(), 'Food')]]/div[contains(@class,'wrap row part')]/span/@alt").extract()).strip(),
            "service_rating": " ".join(response.xpath("//div[@id='RESTAURANT_DETAILS']//div[contains(@class, 'ratingSummary')]//div[contains(@class, 'ratingRow wrap') and ./div[contains(@class, 'label') and contains(./span/text(), 'Service')]]/div[contains(@class,'wrap row part')]/span/@alt").extract()).strip(),
            "value_rating": " ".join(response.xpath("//div[@id='RESTAURANT_DETAILS']//div[contains(@class, 'ratingSummary')]//div[contains(@class, 'ratingRow wrap') and ./div[contains(@class, 'label') and contains(./span/text(), 'Value')]]/div[contains(@class,'wrap row part')]/span/@alt").extract()).strip(),
            "atmosphere_rating": " ".join(response.xpath("//div[@id='RESTAURANT_DETAILS']//div[contains(@class, 'ratingSummary')]//div[contains(@class, 'ratingRow wrap') and ./div[contains(@class, 'label') and contains(./span/text(), 'Atmosphere')]]/div[contains(@class,'wrap row part')]/span/@alt").extract()).strip(),
            "additional_info": " ".join(response.xpath("//div[@class='additional_info']//text()").extract()).strip(),
            "address": " ".join(response.xpath("(//div[contains(@class, 'address')])[1]//text()").extract()).strip(),
            "phone": " ".join(response.xpath("//div[contains(@class, 'phone')]//text()").extract()).strip(),
            "overall_ratting": " ".join(response.xpath("//span[contains(@class, 'overallRating')]//text()").extract()).strip(),
            "n_excellent_review": " ".join(response.xpath("(//div[@id='ratingFilter']//li//*[@class='row_bar']/following-sibling::span)[1]//text()").extract()).strip(),
            "n_very_good_review": " ".join(response.xpath("(//div[@id='ratingFilter']//li//*[@class='row_bar']/following-sibling::span)[2]//text()").extract()).strip(),
            "n_average_review": " ".join(response.xpath("(//div[@id='ratingFilter']//li//*[@class='row_bar']/following-sibling::span)[3]//text()").extract()).strip(),
            "n_poor_review": " ".join(response.xpath("(//div[@id='ratingFilter']//li//*[@class='row_bar']/following-sibling::span)[4]//text()").extract()).strip(),
            "n_terrible_review": " ".join(response.xpath("(//div[@id='ratingFilter']//li//*[@class='row_bar']/following-sibling::span)[5]//text()").extract()).strip(),
            "n_review": " ".join(response.xpath("//a[contains(@class,'seeAllReviews')]//text()").extract()).strip(),
            "tags": ",".join(response.xpath("//span[contains(@class, 'ui_tagcloud')]//text()").extract()).strip(),
        }
        url = response.url
        url_parts = self.url_regex.findall(url)
        last_cmt_page = response.xpath("(//div[@class='pageNumbers'])[1]/span[last()]/text()").extract_first()
        if last_cmt_page:
            for page_number in range(0, int(last_cmt_page) + 1, 10):
                comment_page_url = url_parts[0][0] + "-or" + str(page_number) + "-" + url_parts[0][1]
                yield scrapy.Request(url=comment_page_url, callback=self.extract_comment,
                                    meta = {'res_url': url, 'res_name': res_name})

    def extract_comment(self, response):
        res_url = response.meta['res_url']
        res_name = response.meta['res_name']
        review_without_more = response.xpath(
                "//div[contains(@class, 'reviewSelector') and not(.//span[@class='taLnk ulBlueLinks' and contains(text(), 'More')])]")
        if review_without_more:
            for review in review_without_more:
                user_name = " ".join(review.xpath(".//div[@class='member_info']//div[@class='username mo']/span/text()").extract()).strip()
                cmt_id = " ".join(review.xpath("./@data-reviewid").extract()).strip()
                rating_review_html = review.xpath(".//li[contains(@class, 'recommend-answer')]")
                rating_review_str = ""
                for rating_review in rating_review_html:
                    rating = rating_review.xpath("./div[contains(@class,'ui_bubble_rating')]/@class").extract_first()
                    name = rating_review.xpath("./div[contains(@class,'recommend-description')]//text()").extract_first()
                    if rating and name:
                        rating_review_str += name + ":" + rating + ", "
                yield {
                    'type': 'res_cmt',
                    "user_name": user_name,
                    "user_location": " ".join(review.xpath(".//div[@class='member_info']//div[@class='location']/span/text()").extract()).strip(),
                    "user_rating": " ".join(review.xpath(".//div[@class='rating reviewItemInline']/span[1]/@class").extract()).strip(),
                    "review_time": " ".join(review.xpath(".//div[@class='rating reviewItemInline']/span[2]/text()").extract()).strip(),
                    "quote": " ".join(review.xpath(".//div[@class='quote']//span//text()").extract()).strip(),
                    "comment_link": " ".join(review.xpath(".//div[@class='quote']/a/@href").extract()).strip(),
                    "content": " ".join(review.xpath(".//div[@class='entry']//text()").extract()).strip(),
                    "rating_review": rating_review_str,
                    'res_url': res_url,
                    "res_name": res_name,
                    "cmt_id":  cmt_id,
                }
                sub_cmt = review.xpath(".//div[contains(@class, 'mgrRspnInline')]")
                if sub_cmt:
                    yield {
                        'type': 'res_rep_cmt',
                        "user_name": " ".join(sub_cmt.xpath(
                            ".//div[contains(@class,'prw_reviews_response_header')]/div[@class='header']/text()").extract()).strip(),
                        "user_location": " ",
                        "user_rating": " ",
                        "review_time": " ".join(sub_cmt.xpath(
                            ".//div[contains(@class,'prw_reviews_response_header')]/div[@class='header']/span/text()").extract()).strip(),
                        "quote": " ",
                        "comment_link": " ",
                        "content": " ".join(
                            sub_cmt.xpath(".//p[contains(@class, 'partial_entry')]//text()").extract()).strip(),
                        "rating_review": "",
                        'res_url': res_url,
                        "res_name": res_name,
                        "cmt_id": cmt_id,
                    }
                else:
                    logging.log(logging.INFO, "Can't extracted sub_cmt of  " + cmt_id)
                get_user_info_url = "https://www.tripadvisor.co.nz/MemberOverlay?Mode=owa&uid={0}&c=&src={1}&fus=false&partner=false&LsoId=&metaReferer=Restaurant_Review"
                user_list = response.xpath("//*[@class='reviewSelector']//div[contains(@class, 'memberOverlayLink')]/@id").extract()
                user_info_list = [self.extract_ui_src(user) for user in user_list]
                if user_info_list:
                    for user_info in user_info_list:
                        yield scrapy.Request(url=get_user_info_url.format(user_info[0], user_info[1]), callback=self.get_user_info_parse)
                else:
                    logging.log(logging.ERROR, '[ERROR] Cant find user_info_list at ' + response.url)
        review_with_more = response.xpath("//div[contains(@class, 'reviewSelector') and (.//span[@class='taLnk ulBlueLinks' and contains(text(), 'More')])]")
        if review_with_more:
            reviews_more_id_list = [str(id.replace('review_', '').strip()) for id in review_with_more.xpath("./@data-reviewid").extract()]
            form_data = urllib.urlencode({'reviews': ",".join(reviews_more_id_list)})
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            url = 'https://www.tripadvisor.co.nz/OverlayWidgetAjax?Mode=EXPANDED_HOTEL_REVIEWS&metaReferer=Restaurant_Review'
            yield scrapy.FormRequest(url=url, callback=self.extract_comment, method='POST', headers=headers, body=form_data, meta={'res_url': res_url, 'res_name': res_name})

    def get_user_info_parse(self, response):
        user_profile_url = response.xpath("(//a/@href)[1]").extract_first()
        if user_profile_url:
            yield response.follow(url=user_profile_url, callback=self.user_profile_parse)
        else:
            logging.log(logging.ERROR, "[ERROR] Can't find user_profile_url in " + response.url)

    def user_profile_parse(self, response):
        member_info_container = response.xpath("//div[@id='MODULES_MEMBER_CENTER']")
        user_profile = {
            "type": "user",
            "username": " ".join(member_info_container.xpath(".//span[contains(@class, 'nameText')]/text()").extract()).strip(),
            "join_date": " ".join(member_info_container.xpath(".//p[@class='since']/text()").extract()).strip(),
            "age_or_sex": " ".join(member_info_container.xpath(".//div[@class='ageSince']/p[not(@class='since')]/text()").extract()).strip(),
            "hometown": " ".join(member_info_container.xpath(".//div[@class='hometown']/p/text()").extract()).strip(),
            "about_me_desc": " ".join(member_info_container.xpath(".//div[contains(@class,'aboutMeDesc')]/text()").extract()).strip(),
            "tags": " ".join(member_info_container.xpath(".//div[@class='tagBlock']/div[contains(@class,'tagBubble')]//text()").extract()).strip(),
            "num_reviews": " ".join(member_info_container.xpath(".//div[@class='member-points']//a[@name='reviews']/text()").extract()).strip(),
            "num_ratings": " ".join(member_info_container.xpath(".//div[@class='member-points']//a[@name='ratings']/text()").extract()).strip(),
            "num_forums": " ".join(member_info_container.xpath(".//div[@class='member-points']//a[@name='forums']/text()").extract()).strip(),
            "num_helpful_votes": " ".join(member_info_container.xpath(".//div[@class='member-points']//a[@name='lists']/text()").extract()).strip(),
            "total_points": " ".join(member_info_container.xpath(".//div[@class='memberPointInfo']//div[@class='points']/text()").extract()).strip(),
            "level_of_contributor": " ".join(member_info_container.xpath(".//div[@class='memberPointInfo']//div[@data-info-id='tripcollectiveLevels']/span/text()").extract()).strip(),
        }
        badges_collection_link = member_info_container.xpath(".//a[@class='trophyCase']/@href").extract_first()
        self._save_html(response, response.url)
        if badges_collection_link:
            yield response.follow(badges_collection_link, self.badges_parse, meta={'user_profile': user_profile})

    def badges_parse(self, response):
        badges_container = response.xpath("//div[@class='badgeInfo']")
        badges_list = []
        for badge_tmp in badges_container:
            badge = " ".join(badge_tmp.xpath(".//div[@class='badgeText']/text()").extract()).strip()
            requirement = " ".join(badge_tmp.xpath(".//span[@class='subText']/text()").extract()).strip()
            badges_list.append("-".join([badge, requirement]))
        user_profile = response.meta['user_profile']
        user_profile['badges'] = ",".join(badges_list)
        self._save_html(response, response.url)
        yield user_profile

    def extract_geo_code(self, url):
        geo_code_list = re.findall(r'-g(\d{5,8})-', url)
        geo_code_list_2 = re.findall(r'geo=(\d{5,8})&', url)
        if geo_code_list:
            return geo_code_list[0]
        if geo_code_list_2:
            return geo_code_list_2[0]
        else:
            logging.log(logging.ERROR, "[ERROR] Can't find geo code in " + url)
            return None

    def extract_ui_src(self, user):
        # UID_AAEAC8EBCFF4935B179681F648AF0D66-SRC_505783900
        info = self.user_id_regex.findall(user)
        if info:
            return info[0]
        else:
            return None

    def _save_html(self, response, file_name):
        file_name = file_name.replace('https://www.tripadvisor.co.nz/', '').replace('/', '_fws_').replace('\\', '_bws_')
        html = response.xpath('//html').extract_first().strip()
        file_path = os.path.join(self.base_html_directory, file_name)
        logging.log(logging.INFO, "Crawled from " + file_name)
        with codecs.open(file_path, 'w', "utf-8-sig") as file:
            file.write(html)
