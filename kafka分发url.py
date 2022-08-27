import urllib.request
import random
from lxml import etree
import time
from kafka import KafkaConsumer, KafkaProducer


bootstrap_servers = 'localhost:9092'


class Producer(object):
    def __init__(self, topic):
        self.client = KafkaProducer(bootstrap_servers=['localhost:9092'])
        self.topic = topic

    def send(self, value):
        self.client.send(self.topic, value.encode('utf-8'),partition=0)
        self.client.flush()


class Consumer(object):
    def __init__(self, topic):

        # params = {
        #     'bootstrap_servers': bootstrap_servers,
        #     'auto_commit_interval_ms': 100,
        # }
        self.client = KafkaConsumer(topic,group_id='test_id',bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest')
        self.client.subscribe(topics=[topic, ])

    def __iter__(self):
        return self

    def __next__(self):
        while 1:
            try:
                item = next(self.client)
                return item
            except Exception as e:
                pass

def zhongzi():
    producer = Producer('url')
    url = 'https://sc.chinaz.com/tupian/meishitupian.html'
    producer.send(url)
    for i in range(2, 245):  # 将另外244页的url也保存到 kafka 集合中
            url = 'https://sc.chinaz.com/tupian/meishitupian_' + str(i) + '.html'
            producer.send(url)

def paqu():
    url_c = Consumer('url')
    for item in url_c:
        print(item)
        try:
            url = item.value.decode('utf-8')
            print(url)
            headers = {
                'User-Agent': '',
                'Cookie': ''
                }
            request = urllib.request.Request(url=url, headers=headers)
            free_proxies_pool = [
                    {'http': '223.94.85.131:9091'},
                    {'http': '217.60.194.52:8080'},
                    {'http': '79.122.202.21:8080'},
                    {'http': '18.130.252.7:8888'},
                    {'http': '36.94.174.243:8080'},
                    {'http': '103.119.67.41:3125'},
                ]
            proxies = random.choice(free_proxies_pool)
            handler = urllib.request.ProxyHandler(proxies=proxies)
            opener = urllib.request.build_opener(handler)
            response = opener.open(request)
                # +------------------------------------------------------------------------------------------------------------------------+

                # response = urllib.request.urlopen(request)

            content = response.read().decode('utf-8')
            tree = etree.HTML(content)
            name_list = tree.xpath('//div[@class="item"]/img/@alt')
            src_list = tree.xpath('//div[@class="item"]/img/@data-original')
            for i in range(len(name_list)):
                name = name_list[i]
                src = src_list[i]
                url = 'https:' + src
                print(name)
                print(url)
                urllib.request.urlretrieve(url=url, filename=name + '.jpg')  # 储存模块需要更改
                time.sleep(1)  # 歇一会儿再爬，防止被踢
        except:
            pass
if __name__ == '__main__':
    zhongzi()
    paqu()