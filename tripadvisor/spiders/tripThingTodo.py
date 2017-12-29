# -*- coding: utf-8 -*-
import scrapy
import os
import codecs
import logging
import re
from pprint import pprint
from tqdm import tqdm
import urllib


class TripthingtodoSpider(scrapy.Spider):
    name = 'tripThingTodo'

    custom_settings = {
        'ITEM_PIPELINES': {
            'tripadvisor.pipelines.DuplicatesPipeline': 200,
            'tripadvisor.pipelines.TripThingsTodoPipeline': 300,
        },
        "LOG_LEVEL": 'DEBUG',
        "LOG_FILE": './log_thing.txt',
        "LOG_ENCODING": 'utf-8',
        # "JOBDIR": 'jobs/tripThingTodo-1'
    }

    html_save_path = "D:\\Working\\Crawling_TripVisor_Things"

    def __init__(self):
        super(TripthingtodoSpider, self).__init__()
        # Load geo-location-list
        self.geo_list = []
        with open("geo_code.csv", 'r') as geo_file:
            geo_file.readline()
            for line in geo_file:
                self.geo_list.append(line.strip().split(",")[1])
        self.url_regex = re.compile(
            '(https:\/\/www\.tripadvisor\.co\.nz\/Attraction_Review-g\d+-d\d+-Reviews)-([\w+\-]+\.html)')
        self.url_todo_list_parse = re.compile('(https:\/\/www\.tripadvisor\.co\.nz\/Attractions-g\d+-Activities)-(\w+\.html)')
        self.user_id_regex = re.compile('UID_(\w+)-SRC_(\w+)')
        base_html_directory = os.path.join(self.html_save_path, "html_saved_thing")
        if not os.path.isdir(base_html_directory):
            os.mkdir(base_html_directory)
        self.base_html_directory = base_html_directory

    def start_requests(self):
        geo_list = self.geo_list
        start_url = "https://www.tripadvisor.co.nz/Attractions-g{}-Activities-Vietnam.html"
        for geo_code in geo_list:
            yield scrapy.Request(url = start_url.format(geo_code), callback=self.todo_list_parse, meta={'loop': True})

    def todo_list_parse(self, response):
        self._save_html(response, response.url)
        activity_url_list = response.xpath("//div[@id='FILTERED_LIST']//div[contains(@class, 'listing_title')]//a/@href").extract()
        if activity_url_list:
            for activity_url in activity_url_list:
                if 'Reviews' in activity_url:
                    yield response.follow(url=activity_url, callback=self.extract_things)
                elif 'Activities' in activity_url:
                    yield response.follow(url=activity_url, callback=self.sub_todo_list_parser)
        if response.meta['loop']:
            last_page_index = response.xpath("//div[contains(@class, 'pageNumbers')]/a[last()]//text()").extract_first()
            self.get_all_things_todo_page(last_page_index, response.url)

    def sub_todo_list_parser(self, response):
        self._save_html(response, response.url)
        activity_url_list = response.xpath("//div[contains(@class,'listing_title')]//a/@href").extract()
        if activity_url_list:
            for activity_url in activity_url_list:
                yield response.follow(url=activity_url, callback=self.extract_things)

    def extract_things(self, response):
        self._save_html(response, response.url)
        thing_info_container = response.xpath("//*[@id='taplc_location_detail_header_attractions_0']")
        overview_container = response.xpath("//*[@id='taplc_location_detail_overview_attraction_0']")
        review_container = response.xpath("//*[@id='REVIEWS']")
        url = response.url
        yield {
            'type': 'thing_info',
            "name": " ".join(thing_info_container.xpath(".//*[@id='HEADING']//text()").extract()).strip(),
            "ranking": " ".join(thing_info_container.xpath(".//span[contains(@class, 'header_popularity popIndexValidation')]//text()").extract()).strip(),
            "atraction_detail": " ".join(thing_info_container.xpath(".//div[contains(@class, 'detail')]//text()").extract()).strip(),
            "address": " ".join(thing_info_container.xpath(".//div[contains(@class, 'address')]//text()").extract()).strip(),
            "phone": " ".join(thing_info_container.xpath(".//div[contains(@class, 'phone')]//text()").extract()).strip(),
            "overview": " ".join(overview_container.xpath(".//div[contains(@class, 'description')]//text()").extract()).strip(),
            "overall_rating": " ".join(overview_container.xpath(".//span[contains(@class, 'overallRating')]//text()").extract()).strip(),
            "open_hour": " ".join(overview_container.xpath("//div[contains(@class, 'detail_section hour')]//span[contains(@class, 'time')]//text()").extract()).strip(),
            "suggested_duration": " ".join(overview_container.xpath("//div[contains(@class, 'duration')]//text()").extract()).strip(),
            "n_reviews": " ".join(review_container.xpath(".//div[contains(@class, 'prw_common_location_content_header')]//span[contains(@class, 'reviews_header_count')]//text()").extract()).strip(),
            "n_excellent_review": " ".join(response.xpath("(//*[@id='ratingFilter']//li[contains(@class,'filterItem')]/label/span//text())[1]").extract()).strip(),
            "n_very_good_review": " ".join(response.xpath("(//*[@id='ratingFilter']//li[contains(@class,'filterItem')]/label/span//text())[2]").extract()).strip(),
            "n_average_review": " ".join(response.xpath("(//*[@id='ratingFilter']//li[contains(@class,'filterItem')]/label/span//text())[3]").extract()).strip(),
            "n_poor_review": " ".join(response.xpath("(//*[@id='ratingFilter']//li[contains(@class,'filterItem')]/label/span//text())[4]").extract()).strip(),
            "n_terrible_review": " ".join(response.xpath("(//*[@id='ratingFilter']//li[contains(@class,'filterItem')]/label//text())[5]").extract()).strip(),
            "tags": " ".join(response.xpath("//*[@id='taplc_location_review_keyword_search_0']//span[contains(@class, 'ui_tagcloud') and not (text() = 'All reviews')]//text()").extract()).strip(),
            "thing_url": url
        }
        url_parts = self.url_regex.findall(url)
        last_cmt_page = response.xpath("(//div[@class='pageNumbers'])[1]/span[last()]/text()").extract_first()
        if last_cmt_page:
            for page_number in range(0, int(last_cmt_page) + 1, 10):
                comment_page_url = url_parts[0][0] + "-or" + str(page_number) + "-" + url_parts[0][1]
                yield scrapy.Request(url=comment_page_url, callback=self.extract_comment,
                                     meta={'thing_url': url})

    def extract_comment(self, response):
        self._save_html(response, response.url)
        thing_url = response.meta['thing_url']
        review_without_more = response.xpath(
                "//div[contains(@class, 'reviewSelector') and not(.//span[@class='taLnk ulBlueLinks' and contains(text(), 'More')])]")
        if review_without_more:
            for review in review_without_more:
                user_name = " ".join(review.xpath(".//div[@class='member_info']//div[@class='username mo']/span/text()").extract()).strip()
                cmt_id = " ".join(review.xpath("./@data-reviewid").extract()).strip()
                logging.log(logging.INFO, "[ATRAX_CMT] Crawling from comment " + response.url)
                yield {
                    'type': 'thing_cmt',
                    "user_name": user_name,
                    "user_location": " ".join(review.xpath(".//div[@class='member_info']//div[@class='location']/span/text()").extract()).strip(),
                    "user_rating": " ".join(review.xpath(".//div[@class='rating reviewItemInline']/span[1]/@class").extract()).strip(),
                    "review_time": " ".join(review.xpath(".//div[@class='rating reviewItemInline']/span[2]/text()").extract()).strip(),
                    "quote": " ".join(review.xpath(".//div[@class='quote']//span//text()").extract()).strip(),
                    "comment_link": " ".join(review.xpath(".//div[@class='quote']/a/@href").extract()).strip(),
                    "content": " ".join(review.xpath(".//div[@class='entry']//text()").extract()).strip(),
                    'thing_url': thing_url,
                    "cmt_id":  cmt_id,
                }
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
            yield scrapy.FormRequest(url=url, callback=self.extract_comment, method='POST', headers=headers, body=form_data, meta={'thing_url': thing_url})

    def get_user_info_parse(self, response):
        user_profile_url = response.xpath("(//a/@href)[1]").extract_first()
        if user_profile_url:
            yield response.follow(url=user_profile_url, callback=self.user_profile_parse)
        else:
            logging.log(logging.ERROR, "[03] Can't find user_profile_url in " + response.url)

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

    def get_all_things_todo_page(self, last_page_index, url):
        if not last_page_index and not url:
            return
        url_path = self.url_todo_list_parse.findall(url)
        if url_path:
            last_page_number = int(last_page_index) * 30 + 1
            for i in range(30, last_page_number, 30):
                follow_url = url_path[0][0] + "-oa" + str(i) + "-" + url_path[0][1]
                yield scrapy.Request(url=follow_url, callback=self.todo_list_parse, meta={'loop': False})

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
            logging.log(logging.ERROR, "[02] Can't find geo code in " + url)
            return None

    def extract_ui_src(self, user):
        # UID_AAEAC8EBCFF4935B179681F648AF0D66-SRC_505783900
        info = self.user_id_regex.findall(user)
        if info:
            return info[0]
        else:
            return None

    def _save_html(self, response, file_name):
        file_name = file_name.replace('https://www.tripadvisor.co.nz/', '').replace('/', '_fws_').replace('\\', '_bws_').replace('?', '_qst_')
        html = response.xpath('//html').extract_first().strip()
        file_path = os.path.join(self.base_html_directory, file_name)
        logging.log(logging.INFO, "Crawled from " + file_name)
        with codecs.open(file_path, 'w', "utf-8-sig") as file:
            file.write(html)