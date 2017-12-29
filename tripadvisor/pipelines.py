# -*- coding: utf-8 -*-
from scrapy.exceptions import DropItem
import os
import json
import logging
from pprint import pprint
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class TripadvisorPipeline(object):
    def process_item(self, item, spider):
        return item


class DuplicatesPipeline(object):

    def __init__(self):
        user_file_path = "D:\\Working\\Crawling_TripVisor\\user.jsonl"
        thing_user_file_path = "D:\\Working\\Crawling_TripVisor_Things\\thing_user.jsonl"
        user_list = []
        if os.path.isfile(user_file_path):
            with open(user_file_path, 'r') as user_file:
                for line in user_file:
                    user = json.loads(line)
                    user_list.append(user['username'])
        if os.path.isfile(thing_user_file_path):
            with open(thing_user_file_path, 'r') as thing_user_file:
                for line in thing_user_file:
                    user = json.loads(line)
                    user_list.append(user['username'])
        if user_list:
            self.username_seen = set(user_list)
        else:
            self.username_seen = set()

    def process_item(self, item, spider):
        if item['type'] == 'user':
            if item['username'] in self.username_seen:
                raise DropItem("Duplicate item found: %s" % item)
            else:
                self.username_seen.add(item['username'])
                return item
        else:
            return item


class TripForumPipeline(object):

    def open_spider(self, spider):
        if spider.name == "tripForum":
            self.topic_file = open(os.path.join(spider.html_save_path,'topic.jl'), 'a')
            self.user_file = open(os.path.join(spider.html_save_path,'user.jl'), 'a')
            self.comment_file = open(os.path.join(spider.html_save_path,'comment.jl'), 'a')
        elif spider.name == "geoCodeCrawler":
            self.geo_code_file = open('./geo_code.csv', 'w')
            self.geo_code_file.write('name,code\n')

    def close_spider(self, spider):
        if spider.name == "tripForum":
            self.topic_file.close()
            self.user_file.close()
            self.comment_file.close()

    def process_item(self, item, spider):
        type = item['type']
        del item['type']
        line = json.dumps(dict(item)) + '\n'
        if type == 'topic':
            self.topic_file.write(line)
        elif type == 'comment':
            self.comment_file.write(line)
        elif type == 'user':
            self.user_file.write(line)
        elif type == 'geo_code':
            self.geo_code_file.write(item['loc_name'] + ',' + item['loc_code'] + '\n')
        return item


class TripRestaurantPipeline(object):

    def open_spider(self, spider):
        self.res_info_file = open(os.path.join(spider.html_save_path, 'res_info.jsonl'), 'a')
        self.res_cmt_file = open(os.path.join(spider.html_save_path,'res_cmt.jsonl'), 'a')
        self.user_file = open(os.path.join(spider.html_save_path,'res_user.jsonl'), 'a')
        self.res_rep_cmt_file = open(os.path.join(spider.html_save_path,'res_rep_cmt.jsonl'), 'a')

    def close_spider(self, spider):
        self.res_info_file.close()
        self.res_cmt_file.close()
        self.user_file.close()
        self.res_rep_cmt_file.close()

    def process_item(self, item, spider):
        type = item['type']
        del item['type']
        line = json.dumps(dict(item)) + '\n'
        if type == 'res_info':
            self.res_info_file.write(line)
        elif type == 'res_cmt':
            self.res_cmt_file.write(line)
        elif type == 'user':
            self.user_file.write(line)
        elif type == 'res_rep_cmt':
            self.res_rep_cmt_file.write(line)
        return item


class TripThingsTodoPipeline(object):

    def open_spider(self, spider):
        self.thing_info_file = open(os.path.join(spider.html_save_path,'thing_info.jsonl'), 'a')
        self.thing_cmt_file = open(os.path.join(spider.html_save_path,'thing_cmt.jsonl'), 'a')
        self.user_file = open(os.path.join(spider.html_save_path,'thing_user.jsonl'), 'a')

    def close_spider(self, spider):
        self.thing_info_file.close()
        self.thing_cmt_file.close()
        self.user_file.close()

    def process_item(self, item, spider):
        type = item['type']
        del item['type']
        line = json.dumps(dict(item)) + '\n'
        if type == 'thing_info':
            self.thing_info_file.write(line)
        elif type == 'thing_cmt':
            self.thing_cmt_file.write(line)
        elif type == 'user':
            self.user_file.write(line)
        return item