# -*- coding: utf-8 -*-
import scrapy

from novel.items import NovelItem


class SolveSpider(scrapy.Spider):
    name = "solve"
    allowed_domains = ["qidian.com"]
    start_urls = [];
    for x in range(1,5):#只有5页
        start_urls.append("https://www.qidian.com/all?orderId=&style=1&pageSize=20&siteid=1&pubflag=0&hiddenField=0&page=" + str(x))
    #start_urls = ["https://www.qidian.com/all?orderId=&style=1&pageSize=20&siteid=1&pubflag=0&hiddenField=0&page="]
   # page_index = ["1", "2", "3", "4", "5", "6", "7","8", "9", "10"]
    def parse(self, response):
        nolves = response.xpath('//ul[@class="all-img-list cf"]/li')
        for each in nolves:
           # print("***************************")

            item = NovelItem()
            part = each.xpath('./div[@class="book-mid-info"]')
            #print(part)
            item['bookname'] = part.xpath('./h4/a/text()').extract()
            item['link'] = part.xpath('./h4/a/@href').extract()[0]
            item['author'] = part.xpath('./p[@class="author"]/a[@class="name"]/text()').extract()
            item['category'] = part.xpath('./p[@class="author"]/a/text()').extract()
            item['content'] = part.xpath('./p[@class="intro"]/text()').extract()
            yield item
