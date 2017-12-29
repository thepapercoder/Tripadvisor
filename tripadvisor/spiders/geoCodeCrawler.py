# -*- coding: utf-8 -*-
import scrapy
import logging
import re

class GeocodecrawlerSpider(scrapy.Spider):
    name = 'geoCodeCrawler'
    # allowed_domains = ['www.tripadvisor.co.nz/Attractions-g293921-Activities-Vietnam.html']
    # start_urls = ['http://www.tripadvisor.co.nz/Attractions-g293921-Activities-Vietnam.html/']

    def start_requests(self):
        base_url_firt = "https://www.tripadvisor.co.nz/Attractions-g293921-Activities-Vietnam.html"
        base_url_list = ["https://www.tripadvisor.co.nz/Attractions-g293921-Activities-oa20-Vietnam.html",
                         "https://www.tripadvisor.co.nz/Attractions-g293921-Activities-oa70-Vietnam.html"]
        yield scrapy.Request(url=base_url_firt, callback=self.parse, meta={'first_page': True})
        for url in base_url_list:
            yield scrapy.Request(url=url, callback=self.parse, meta={'first_page': False})

    def parse(self, response):
        is_first_page = response.meta['first_page']
        if is_first_page:
            loc_list_xpath = "((//div[@class='navigation_list'])[2]//a)[position() != last()]"
        else:
            loc_list_xpath = "//ul[@class='geoList']/li/a"
        loc_list = response.xpath(loc_list_xpath)
        for loc in loc_list:
            loc_name = self.extract_loc_name(loc.xpath('./text()').extract_first())
            loc_code = self.extract_geo_code(loc.xpath('./@href').extract_first())
            yield {
                'type': 'geo_code',
                'loc_name': loc_name,
                "loc_code": loc_code
            }

    def extract_loc_name(self, name_raw):
        return name_raw.replace('Things to do in ', '').replace('attractions', '')

    def extract_geo_code(self, url):
        geo_code_list = re.findall(r'-g(\d{5,8})-', url)
        if geo_code_list:
            return geo_code_list[0]
        else:
            logging.log(logging.ERROR, "[02] Can't find geo code in " + url)
            return None