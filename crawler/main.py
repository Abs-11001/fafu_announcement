import re
import time

import requests
from bs4 import BeautifulSoup
import pymysql
from datetime import datetime
import uuid

host = "47.99.64.125"
# host = "localhost"
port = 3306
user = "FAFU"
psd = "fafu123456"
database = "fafu"

header = {
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding':'gzip, deflate, br',
    'Accept-Language':'zh-CN,zh;q=0.9',
    'Cache-Control':'max-age=0',
    'Connection':'keep-alive',
    'Cookie':'JSESSIONID=8BC1C4BCA4C9B7272655611B4CF5A395; language=',
    'Host':'www.fafu.edu.cn',
    'Sec-Fetch-Dest':'document',
    'Sec-Fetch-Mode':'navigate',
    'Sec-Fetch-Site':'none',
    'Sec-Fetch-User':'?1',
    'Upgrade-Insecure-Requests':'1',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'sec-ch-ua':'"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"',
    'sec-ch-ua-mobile':'?0',
    'sec-ch-ua-platform':'"Windows"',
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
        self.repeatCount = 0

    def getPlateKeys(self):
        """
        获取要爬取的部门板块信息
        :return: 从数据库中加载部门板块信息
        """
        sql = "select idx, department, plate, url from PlateKeys"
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
        # 重试次数+1
        self.repeatCount += 1
        # 如果重试超过3次将不再重试
        if self.repeatCount >= 3:
            self.repeatCount = 0
            return None
        try:
            r = requests.get(url, timeout=TIMEOUT, headers=header, verify=False)
            r.encoding = r.apparent_encoding
            soup = BeautifulSoup(r.text, 'html.parser')
            if len(str(soup)) == 0:
                return self.getSoup(url)
            self.repeatCount = 0
            return soup
        except Exception as e:
            log("{}---------------页面解析失败".format(url))
            return self.getSoup(url)

    def checkExist(self, url):
        """
        判断是否已经存入过数据库
        存在返回 True
        不存在返回 False
        """
        sql = "select idx from Announcement where url = %s"
        try:
            self.cursor.execute(sql, url)
            cnt = self.cursor.rowcount
            if cnt > 0:
                return True
        except Exception as e:
            self.db.rollback()
            print(e)
            print("{}---------------查询是否存在失败".format(url))
        return False

    def insert(self, uid, title, url, date, plate, department):
        # 插入通知公告信息到announcement数据库
        addTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "insert into Announcement (uuid,title,url,date,plate,department,addTime) values (%s,%s,%s,%s,%s,%s,%s)"
        try:
            self.cursor.execute(sql, (uid, title, url, date, plate, department, addTime))
            self.db.commit()
            print("{}---------------插入成功".format(title))
        except Exception as e:
            self.db.rollback()
            print(e)
            print("{}--{}-------------插入失败".format(title, url))
            log("{}--{}-------------#插入失败".format(title, url))

    def getAssignPage(self, url, department, plate):
        """
        用于当获取数据时因为网络问题、连接超时问题...而导致页面解析失败的获取指定页面数据的方法
        :param url: 获取的指定url
        :param department: 该url所属的部门
        :param plate: 该url所属的板块
        :param isContinueGet: 该url获取完之后是否继续获取数据，默认True，即获取完之后会继续往后获取数据
        :param isBack: 继续获取数据时url的规律，通常url的顺序为正序，即list1.html、list2.html。。。,不免部分页面顺序为倒序，即list10.html、list9.html
        :return:
        """
        if department == '官网':
            page = re.findall(r'list(.*?).htm', url)[0]
            self.parseHome(department, plate, url, page)


    def parseHome(self, department, plate, target_url, begin_page = 1):
        soup = self.getSoup(target_url)
        # 获取该板块的总页码
        all_pages_element = soup.find('em', class_='all_pages')
        if all_pages_element is not None:
            # 解析页码值
            all_pages = int(all_pages_element.string)
            # 标志是否已经获取过该页公告
            existFlag = False
            for idx in range(int(begin_page), all_pages + 1):
                # 如果existFlag被标识为True说明该页某些数据已被获取过
                if existFlag is True:
                    break
                # 构造新请求地址
                # 加入正则'\d*'的意义是，除了正常的获取数据外，还会出现指定url获取数据，这时需要将指定的url中的页码去掉
                new_url = re.sub(r'\d*.htm', str(idx) + '.htm', target_url)
                print('---------------------------------------------')
                print(new_url)
                print('---------------------------------------------')
                soup = self.getSoup(new_url)
                if soup is None:
                    break
                # 获取公告内容区
                announcement_list = soup.find('div', id='wp_news_w9')
                # 获取公告url、title、date数据
                urls = announcement_list.findAll('a', class_='column-news-item')
                titles = announcement_list.findAll('span', class_='column-news-title')
                dates = announcement_list.findAll('span', class_='column-news-date')
                for i in range(len(urls)):
                    link = str(urls[i]['href'])
                    if link.find('http:') == -1:
                        link = 'https://www.fafu.edu.cn' + link
                    title = titles[i].string
                    date = dates[i].string

                    if self.checkExist(link):
                        existFlag = True
                        print("{}-{}----{}  {}   {}------------------>已存在，将跳过该新闻  ".format(department, plate,
                                                                                                    title, link, date))
                        # 修改如果监测到已经存在了，防止某些新闻被置顶，不立即退出循环
                        # break
                    else:
                        existFlag = False
                        uid = uuid.uuid4()
                        self.insert(uid, title, link, date, plate, department)
                        # self.getPageData(uid, department, link)
                        print("{}-{}----{}  {}   {}".format(department, plate, title, link, date))
                time.sleep(SLEEP)
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
    # fafu.getAssignPage('https://www.fafu.edu.cn/5299/list17.htm', '官网', '上级政策')
    # 关闭FAFU相关连接
    fafu.close()
