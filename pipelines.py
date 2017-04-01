# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from sqlalchemy import *


class SqlitePipeline(object):
    def process_item(self, item, spider):
        engine = create_engine('sqlite:///./weather/static/weather.db')  # 定义引擎
        metadata = MetaData(engine)
        hourly_table = Table('hourly', metadata, autoload=True)
        i = hourly_table.insert()

        i.execute(time=item['time'],
                  summary=item['summary'],
                  temperature=item['temperature'],
                  district=spider.district,
                  lat=spider.lat,
                  lon=spider.lon,
                  cloudCover=item['cloudCover'],
                  humidity=item['humidity'],
                  pressure=item['pressure'],
                  windSpeed=item['windSpeed'],
                  visibility=item['visibility'],
                  precipIntensity=item['precipIntensity'],
                  precipProbability=item['precipProbability'])
        return item
