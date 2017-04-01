# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.

from datetime import datetime, timedelta
import scrapy.spiders, json
from pprint import pprint

from ..items import WeatherItem


class weatherSpider(scrapy.spiders.Spider):
    name = 'darkNetSpider'
    allowed_domains = ['api.darksky.net']
    start_urls = []
    district = None
    lat = None
    lon = None

    def __init__(self, lat, lon, start_date, end_date, tokens_path, district, *args, **kwargs):
        super(weatherSpider, self).__init__(*args, **kwargs)
        self.district = district
        self.lat = lat
        self.lon = lon
        with open(tokens_path) as tokens_file:
            # tokens variable like [[token_str,called_times],[token_str,called_times],...]
            tokens = [{'token': x.split(',')[0], 'called_times': int(x.split(',')[1])} for x in tokens_file.readlines()]
            assert len(tokens) > 0
        # base url for building start urls
        base_url = 'https://api.darksky.net/forecast/{token}/{lat},{lon},{date}T00:00:00?exclude=currently,flags&lang=zh&units=ca'
        #
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
        # variable for index of the using token
        using_token_index = 0
        # variable for number of requests of this Object
        original_requests_number = (end_datetime - start_datetime).days + 1
        actual_request_number = 0

        while (using_token_index != len(tokens)):
            this_token = tokens[using_token_index]['token']
            this_called_times = tokens[using_token_index]['called_times']
            #
            this_dates_length = min((end_datetime - start_datetime).days + 1, 995 - this_called_times)
            tokens[using_token_index]['called_times'] += this_dates_length
            #
            this_dates = [str(start_datetime.date() + timedelta(i)) for i in range(this_dates_length)]
            self.start_urls += [base_url.format(lat=lat, lon=lon, date=x, token=this_token) for x in this_dates]
            start_datetime = start_datetime + timedelta(this_dates_length)
            #
            actual_request_number += this_dates_length
            using_token_index += 1

        print('Original request number: {ori_req}\nActual Request number: {act_req}'.format(
            ori_req=original_requests_number,
            act_req=actual_request_number))
        print('Tokens Used Condition:')
        with open(tokens_path, 'w') as tokens_file:
            for token in tokens:
                tokens_file.write('%s,%d\n' % (token['token'], token['called_times']))
                print('%s:%d times used' % (token['token'], token['called_times']))

    def parse(self, response):
        raw_data = json.loads(response.body)
        items = []
        for hour in raw_data['hourly']['data']:
            item = WeatherItem()
            for key in list(item.fields.keys()):
                if key in hour:
                    item[key] = hour[key]
                else:
                    item[key] = None
            item['district']= self.district
            items.append(item)
        return items
