# -*- coding: utf-8 -*-
import scrapy
import os
import codecs
import logging
from tqdm import tqdm

class TripforumSpider(scrapy.Spider):
    name = 'tripForum'
    # allowed_domains = ['https://www.tripadvisor.co.nz/ShowForum-g293921-i8432-Vietnam.html']
    # start_urls = ['https://www.tripadvisor.co.nz/ShowForum-g293921-i8432-Vietnam.html/']

    ITEM_TYPE = ['topic', 'comment', 'user']
    html_save_path = "D:\\Working\\Crawling_TripVisor"

    custom_settings = {
        'ITEM_PIPELINES': {
            'tripadvisor.pipelines.DuplicatesPipeline': 200,
            'tripadvisor.pipelines.TripForumPipeline': 300,
        },
        "LOG_LEVEL": 'DEBUG',
        "LOG_FILE": './log_forum.txt',
        "LOG_ENCODING": 'utf-8',
        "JOBDIR": 'jobs/tripForum-1'
    }


    def __init__(self):
        super(TripforumSpider, self).__init__()
        base_html_directory = os.path.join(self.html_save_path, "html_saved")
        if not os.path.isdir(base_html_directory):
            os.mkdir(base_html_directory)
        self.base_html_directory = base_html_directory

    def start_requests(self):
        url = 'https://www.tripadvisor.co.nz/ShowForum-g293921-i8432-Vietnam.html/'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        info_table = response.xpath("//table[@id='SHOW_FORUMS_TABLE']//tr")
        for row in tqdm(info_table):
            topic_link = row.xpath("./td[3]/b/a[1]/@href").extract_first()
            return_item =  {
                "forum": " ".join(row.xpath("./td[2]//text()").extract()).strip(),
                "topic": " ".join(row.xpath("./td[3]//text()").extract()).strip(),
                "replies": " ".join(row.xpath("./td[4]//text()").extract()).strip(),
                "last_post": " ".join(row.xpath("./td[5]//text()").extract()).strip(),
            }
            if topic_link:
                return_item["topic_url"] = topic_link
                yield response.follow(topic_link, self.comment_parse, meta={"topic": return_item['topic'], "topic_url": topic_link})
                yield response.follow(topic_link, self.topic_detail_parse, meta={"item": return_item}, dont_filter=True)
        next_page = response.xpath("//a[@class='guiArw sprite-pageNext']/@href").extract_first()
        if next_page:
            yield response.follow(next_page, self.parse)
        self._save_html(response, response.url)
        self.log("Crawling from " + response.url)

    def topic_detail_parse(self, response):
        return_item = response.meta['item']
        topic_container = response.xpath("//div[@class='bx01']")
        post_title = " ".join(topic_container.xpath('.//span[@class="topTitleText"]//text()').extract()).strip()
        post_date = " ".join(topic_container.xpath('.//div[@class="postDate"]//text()').extract()).strip()
        post_content = " ".join(topic_container.xpath('.//div[@class="postBody"]//text()').extract()).strip()
        owner_profile_container = topic_container.xpath(".//div[@class='profile']")
        owner_name = " ".join(owner_profile_container.xpath('.//div[@class="username"]/a//text()').extract()).strip()
        owner_profile_url = " ".join(owner_profile_container.xpath('.//div[@class="username"]/a/@href').extract()).strip()
        yield {
            "type": TripforumSpider.ITEM_TYPE[0],
            "forum": return_item['forum'],
            "topic": return_item['topic'],
            "topic_url": return_item['topic_url'],
            "replies": return_item['replies'],
            "last_post": return_item['last_post'],
            "post_title": post_title,
            "post_date": post_date,
            "post_content": post_content,
            "owner_name": owner_name,
            "owner_profile_url" : owner_profile_url
        }
        self._save_html(response, response.url)
        if owner_profile_url != "":
            yield response.follow(owner_profile_url, self.user_profile_parse)

    def comment_parse(self, response):
        topic = response.meta['topic']
        topic_url = response.meta['topic_url']
        replies_container = response.xpath("//div[@id='SHOW_TOPIC']/div[@class='balance']/div[contains(@class, 'post')]")
        for replie in replies_container:
            owner_name = " ".join(replie.xpath(".//div[@class='username']/a//text()").extract()).strip()
            owner_profile_url = " ".join(replie.xpath(".//div[@class='username']/a/@href").extract()).strip()
            replie_date = " ".join(replie.xpath(".//div[@class='postDate']/text()").extract()).strip()
            replie_content = " ".join(replie.xpath(".//div[@class='postBody']/p//text()").extract()).strip()
            yield {
                "type": TripforumSpider.ITEM_TYPE[1],
                "topic": topic,
                "topic_url": topic_url,
                "owner_name": owner_name,
                "owner_profile_url": owner_profile_url,
                "replie_date": replie_date,
                "replie_content": replie_content,
                "comment_url": response.url.replace('https://www.tripadvisor.co.nz/', '')
            }
        self._save_html(response, response.url)
        next_page = response.xpath("(//div[@id='pager_top2']/a[contains(@class, 'sprite-pageNext')]/@href)[1]").extract_first()
        if next_page:
            yield response.follow(next_page, self.comment_parse, meta={'topic': topic, 'topic_url': topic_url})

    def user_profile_parse(self, response):
        member_info_container = response.xpath("//div[@id='MODULES_MEMBER_CENTER']")
        user_profile = {
            "type": TripforumSpider.ITEM_TYPE[2],
            "username": " ".join(member_info_container.xpath(".//span[contains(@class, 'nameText')]/text()").extract()).strip(),
            "join_date": " ".join(member_info_container.xpath(".//p[@class='since']/text()").extract()).strip(),
            "age_or_sex": " ".join(member_info_container.xpath(".//div[@class='ageSince']/p[not(@class='since')]/text()").extract()).strip(),
            "hometown": " ".join(member_info_container.xpath(".//div[@class='hometown']/p/text()").extract()).strip(),
            "about_me_desc": " ".join(member_info_container.xpath(".//div[contains(@class,'aboutMeDesc')]/text()").extract()).strip(),
            "tags": " ".join(member_info_container.xpath(
                ".//div[@class='tagBlock']/div[contains(@class,'tagBubble')]//text()").extract()).strip(),
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

    def _save_html(self, response, file_name):
        file_name = file_name.replace('https://www.tripadvisor.co.nz/', '').replace('/', '_fws_').replace('\\', '_bws_')
        html = response.xpath('//html').extract_first().strip()
        file_path = os.path.join(self.base_html_directory, file_name)
        logging.log(logging.INFO, "Crawled from " + file_name)
        with codecs.open(file_path, 'w', "utf-8-sig") as file:
            file.write(html)