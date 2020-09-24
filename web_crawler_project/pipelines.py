# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pymongo import MongoClient


class WebCrawlerProjectPipeline:
    def process_item(self, item, spider):
        item['push'] = int(item['push'])
        
        return item

class DeleteNullTitlePipeline(object):
    def process_item(self, item, spider):
        title = item['title'] 
        if title:
            return item
        else:
            raise DropItem('found null title %s', item)    
    
class DuplicatesTitlePipeline(object):
    def __init__(self):
        self.article = set()
    def process_item(self, item, spider):
        title = item['title'] 
        if title in self.article:
            raise DropItem('duplicates title found %s', item)
        self.article.add(title)
        return(item)
    
class MongoDBPipeline:
    
    # 開始爬之前先連接MongoDB並設定參數
    def open_spider(self, spider):
        db_uri = spider.settings.get('MONGODB_URI', 'mongodb://localhost:27017')
        db_name = spider.settings.get('MONGODB_DB_NAME', 'ptt_scrapy')
        self.db_client = MongoClient('mongodb://localhost:27017')
        self.db = self.db_client[db_name]
    
    # 呼叫insert_article
    def process_item(self, item, spider):
        self.insert_article(item)
        return item
    
    # 新增資料到MongoDB內
    def insert_article(self, item):
        item = dict(item)
        self.db.article.insert_one(item)
    
    # 爬取完全部後被呼叫，關閉連接
    def close_spider(self, spider):
        self.db_clients.close()