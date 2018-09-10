# -*- coding: utf-8 -*-
# 使用requests和BeautifulSoup爬取链家网北京二手房前100页信息，此外使用time来控制爬取频率，使用pandas对爬取内容处理并做表保存，使用progressbar2输出爬取进度
import requests
import time
import pandas as pd
import progressbar
import datetime as dt
from bs4 import BeautifulSoup

# 目标网站URL结构如下：https://bj.lianjia.com/ershoufang/pg1，bj表示城市，ershoufang表示当前子目录是二手房，pg1表示第一页
# 设置URL固定部分
url = 'https://bj.lianjia.com/ershoufang/'
# 设置页面可变部分
page = ('pg')

# 发送http请求时最好设置一个虚构的头部信息，否则很容易被反爬虫封IP。设置请求头部信息如下
headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
'Accept':'text/html;q=0.9,*/*;q=0.8',
'Accept-Charset':'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
'Accept-Encoding':'gzip',
'Connection':'close',
'Referer':'http://www.baidu.com/link?url=_andhfsjjjKRgEWkj7i9cFmYYGsisrnm2A-TN3XZDQXxvGsM9k9ZZSnikW2Yds4s&amp;amp;wd=&amp;amp;eqid=c3435a7d00146bd600000003582bfd1f'
}

progressbar.streams.flush()
# 使用for循环生成1-100的数字，转化格式后于前面的URL固定部分拼接成实际URL，并且设置抓取频率为间隔1秒，并用html拼接保存所有request contents
html = 0
iterator_times = 100
for i in progressbar.progressbar(range(iterator_times)):
    i = str(i)
    link = (url + page + i + '/')
    req = requests.get(url=link, headers=headers)
    html = req.content
    if i == 1:
        html = html
    else:
        html2 = req.content
        html = html + html2
    # 间隔1秒
    time.sleep(1)
    # 不换行输出爬取进度，\r表示回车，这里即字符输出回到该行头部，end=''代表不换行，flush=True代表控制台刷新
    # print('\r{0}/100'.format(i+1) + end-start + 'ms', end='', flush=True)

# 使用BeautifulSoup对HTML文本进行解析
soup = BeautifulSoup(html, 'html.parser')

# 下面分别提取房源价格、房屋信息、关注度、地理位置、关键标签等内容
# 提取房源总价
totalPrice_html = soup.findAll('div', 'totalPrice')
totalPrice = []
for temp in totalPrice_html:
    totalPrice.append(temp.span.string)

# 提取房屋信息
houseInfo_html = soup.findAll('div', 'houseInfo')
houseInfo = []
for temp in houseInfo_html:
    houseInfo.append(temp.get_text())

# 提取地理位置
positionInfo_html = soup.findAll('div', 'positionInfo')
positionInfo = []
for temp in positionInfo_html:
    positionInfo.append(temp.get_text())

# 提取关注度
followInfo_html = soup.findAll('div', attrs={'class':'followInfo'}) # 与上面等价的写法
followers = []
times = []
# 由于链家页面结构改变，导致 关注人数 与 看房次数 不是并列的层次，只好用contents来获取整个标签里的信息，再来按位置抓取
for temp in followInfo_html:
    followers.append(temp.contents[0])
    times.append(temp.contents[2])

# 获取每个房源的标签信息，包括三项：进地铁、五年房本、随时看房
# 这些标签在class为tag的html标签下存储，除了显式的用户能看到的class='tag'的内容外，页面源码又输出了一次class='tag'的隐式内容
# 换句话说，class为tag的内容出现了两次，我们需要去除重复的内容，恰好两次class='tag'的内容挂靠在不同的父class下，这里利用父class去重
tag_html = soup.findAll('div', 'tag')
subway = []
taxfree = []
haskey = []
for temp in tag_html:
    # 判断class='tag'的html标签的父class是否为'info clear'，是才计算，从而避免计算两次
    if(temp.find_parent('div', 'info clear') is not None):
        subway.append(temp.find('span', 'subway') is not None)
        taxfree.append(temp.find('span', 'taxfree') is not None)
        haskey.append(temp.find('span', 'haskey') is not None)

# 我们汇总并清洗数据，将数据利用pandas整理到表中，
house = pd.DataFrame(data={'totalPrice': totalPrice, 'houseInfo': houseInfo, 'positionInfo': positionInfo,\
                           'followers': followers, 'times': times, 'subway': subway, 'taxfree': taxfree, 'haskey': haskey})

# 上面的houseInfo、positionInfo等信息是混杂的，我们这里对这两块信息进行数据清洗，每个条目的信息的子信息之间都以'/'分割，我们对此做处理即可
# 对于绝大多数房屋信息数据，都只包含6列内容：小区、户型、面积、朝向、装修、有无电梯。但是存在极少部分信息小区由两块表示，导致其他信息整体后移1列，从而包含7列内容
# 我们先把7列的数据转化成6列数据
split_list = []
for x in house.houseInfo:
    spt = x.split('/')
    if len(spt) == 6:
        split_list.append(spt)
    else:
        spt[0] = spt[0] + spt[1]
        spt.pop(1)
        split_list.append(spt)

# print(split_list[:10])
# 构建houseInfo的DataFrame
houseInfo_split = pd.DataFrame(data = split_list,
                               index = house.index,
                               columns = ['village', 'type', 'area', 'orientation', 'decoration', 'elevator'])
# 将两个DataFrame合并
house = pd.merge(house, houseInfo_split, left_index=True, right_index=True)

# 再对positionInfo处理，包含3列内容：楼层、建成时间与楼型、地区
split_list = []
for x in house.positionInfo:
    spt = x.split('/')
    split_list.append(spt)
# 构建positionInfo的DataFrame
positionInfo_split = pd.DataFrame(data = split_list,
                                  index = house.index,
                                  columns = ['floor', 'builtTimeAndType', 'District'])
# 将两个DataFrame合并
house = pd.merge(house, positionInfo_split, left_index=True, right_index=True)
# positionInfo和houseInfo的信息我们已另外保存到新的字段，现在就可以删除这两列
house.drop(axis=1, columns='houseInfo', inplace=True)
house.drop(axis=1, columns='positionInfo', inplace=True)

# 保存成csv并在打印台输出部分信息
house.to_csv('lianjia_bj_ershoufang' + dt.datetime.now().strftime('%Y%m%d%H') +'.csv')
print(house.head())