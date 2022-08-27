import urllib.request
import random
from lxml import etree
import redis
import time

pool= redis.ConnectionPool(host='localhost',port=6379,db=0,decode_responses=True) # host为主机的IP，端口为6379，数据库为默认，字符串格式
job_redis=redis.Redis(connection_pool=pool)

class Fbspc(object):

    # 数据初始化,创建种子url集合

    def __init__(self):
        url = 'https://sc.chinaz.com/tupian/meishitupian.html'
        job_redis.sadd('urls', url)#将第一页的url保存到redis集合中
        for i in range(2, 245):# 将另外244页的url也保存到redis集合中
            url = 'https://sc.chinaz.com/tupian/meishitupian_' + str(i) + '.html'
            job_redis.sadd('urls', url)
        self.main()

    # （1）请求对象的定制及请求访问
    def create_request(self,url):
        headers = {
            'User-Agent': '',
            'Cookie': ''
        }
        request = urllib.request.Request(url=url, headers=headers)
        self.get_content(request)

    # （2）获取服务器响应数据
    def get_content(self,request):
        # +------------------------------------------------------------------------------------------------------------------------+
        # 校验时间 2022-8-15  14:56 ,若代理池不可用，注释掉分割线内的内容，并将分割线下方的response取消注释
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
        self.download_clean(content)

    # （3）下载及清洗数据
    def download_clean(self,content):
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
            time.sleep(1)#歇一会儿再爬，防止被踢


    def main(self):
        url = job_redis.spop('urls')
        while url!=None:
            try:
                self.create_request(url)
            except Exception as e:
                if hasattr(e, "reason"):
                    print(e.reason)
            url = job_redis.spop('urls')
        print('已爬取完毕')


if __name__ == '__main__':
    job1=Fbspc()