# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import  pymongo
from kafka import SimpleProducer
from kafka.client_async import KafkaClient
from scrapy.utils.serialize import ScrapyJSONEncoder

list = []
class NovelPipeline(object):
    def process_item(self, item, spider):
        global list
        list = []
        with open("E://nolves.txt", 'a') as fp:
            list.append(item['category'][1])
            list.append(item['author'][0])
            list.append(item['link'])
            list.append(item['bookname'][0])
            list.append(item['content'][0].strip())

           # print(bookname + "\t" + author + "\t" + category + "\t"+ content + "\t" + link + "\n")
            fp.write(" ".join(list) + '\n')

class MongoDBPipeline(object):
    collection_name = 'scrapy_items'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db


    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB'),
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.collection = self.db["novel"]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        global list
        list = []
        list.append(item['category'][1])
        list.append(item['author'][0])
        list.append(item['link'])
        list.append(item['bookname'][0])
        list.append(item['content'][0].strip())
        print(list)
        self.collection.insert(dict(item))
        print("插入成功")
        return item



class KafkaPipeline(object):

    #初始化配置Kafka
    def __init__(self, producer, topic):
        self.producer = producer
        self.topic = topic
        self.encoder = ScrapyJSONEncoder()

    #处理和编码每条数据记录，并发送给Kafka

    def process_item(self, item, spider):
        global list
        list = []
        list.append(item['category'][1])
        list.append(item['author'][0])
        list.append(item['link'])
        list.append(item['bookname'][0])
        list.append(item['content'][0].strip())
        item = dict(list)
        item['spider'] = spider.name
        msg = self.encoder.encode(item)
        self.producer.send_messages(self.topic, msg)

    #初始化配置，并创建客户端和调用写入Kafka函数逻辑
    @classmethod
    def from_settings(cls, settings):
        k_hosts = settings.get('KAFKA_HOSTS', ['192.168.177.11:9092','192.168.177.12:9092','192.168.177.13:9292'])
        topic = settings.get('KAFKA_TOPIC', 'nolves')
        kafka = KafkaClient(k_hosts)
        conn = SimpleProducer(kafka)
        return cls(conn, topic)

