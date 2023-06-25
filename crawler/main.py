import requests
from bs4 import BeautifulSoup
import pymysql
from datetime import datetime
import time

# host = "47.99.64.125"
host = "localhost"
port = 3306
user = "root"
psd = "zsl20001101"
database = "FAFU"

header = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'max-age=0',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'
}

# 统一睡眠时间
SLEEP = 5
# 统一超时时间
TIMEOUT = 5


def log(info):
    try:
        logTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open("log.txt", "a+") as file:
            file.write(logTime + ":")
            file.write("\n")
            file.write(info)
            file.write("\n\n")
            file.close()
    except Exception as e:
        print(e)


class FAFU:
    def __init__(self):
        """
        创建数据库连接
        """
        self.db = pymysql.connect(host=host, port=port, user=user, passwd=psd, database=database)
        self.cursor = self.db.cursor()

    def getPlateKeys(self):
        """
        获取要爬取的部门板块信息
        :return: 从数据库中加载部门板块信息
        """
        sql = "select idx, department, plate, url from plateKeys"
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except Exception as e:
            print(e)
            print("---------------查询机构keys失败")
            log("查询机构keys失败：" + e)
            return None
        return False

    def getSoup(self, url):
        try:
            r = requests.get(url, timeout=TIMEOUT, headers=header)
            r.encoding = r.apparent_encoding
            soup = BeautifulSoup(r.text, 'html.parser')
            if len(str(soup)) == 0:
                return None
            return soup
        except Exception as e:
            log("{}---------------页面解析失败".format(url))
            return None

    def parseHome(self, department, plate, target_url):
        soup = self.getSoup(target_url)
        # 获取该板块的总页码
        all_pages_element = soup.find('em', class_='all_pages')
        if all_pages_element is not None:
            # 解析页码值
            all_pages = int(all_pages_element.string)
            for idx in range(16, all_pages + 1):
                # 构造新请求地址
                new_url = target_url.replace('.htm', str(idx) + '.htm')
                print('---------------------------------------------')
                print(new_url)
                print('---------------------------------------------')
                soup = self.getSoup(new_url)
                # 获取公告内容区
                announcement_list = soup.find('div', id='wp_news_w9')
                # 获取公告url、title、date数据
                urls = announcement_list.findAll('a', class_='column-news-item')
                titles = announcement_list.findAll('span', class_='column-news-title')
                dates = announcement_list.findAll('span', class_='column-news-date')
                for i in range(len(urls)):
                    url = urls[i]['href']
                    title = titles[i].string
                    date = dates[i].string
                    print(title, url, date)
                # break






    def begin(self):
        """
        FAFU功能执行入口
        :return:
        """
        plateKeys = self.getPlateKeys()
        for key in plateKeys:
            department = key[1]
            plate = key[2]
            url = key[3]
            if department == '官网':
                self.parseHome(department, plate, url)

        print(plateKeys)

    def close(self):
        """
        关闭数据库连接
        """
        self.cursor.close()
        self.db.close()


if __name__ == '__main__':
    # 实例化FAFU对象
    fafu = FAFU()
    # 启动FAFU功能
    fafu.begin()
    # 关闭FAFU相关连接
    fafu.close()
