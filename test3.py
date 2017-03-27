import re
import traceback

import requests
import pandas as pd
import time
from bs4 import BeautifulSoup

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'}


def download(url, headers):
    """
        连接并下载网页，最后返回的是经过处理的HTML文本
        :param url:传入的年鉴URL
        :param k:年份，用于判断网页的编码方式
        :param headers:
        :return:HTML文本
        """
    try:
        html = requests.get(url, headers=headers, timeout=30)
        time.sleep(2)
        html.encoding = 'utf-8'
        state = html.status_code
        # print(html.text)expires
    except:
        print('链接超时')
        html = None
    return html.text, state


def create_table(html):
    """
        传入HTML将其转变为可以变成表格的自适应二维数组，自适应二维数组是为了方便下面的表格拼接工作
        :param html:经过筛选的表格html
        :return:返回一个只有表格数据的二维数组
        """
    soup = BeautifulSoup(html, 'lxml')
    tr = soup.find_all('tr')
    rows = len(tr)
    cols = len(tr[0].find_all('td'))
    for td_len in tr:
        if len(td_len.find_all('td')) > cols:
            cols = len(td_len.find_all('td'))
    lists = [[None] * cols for i in range(rows)]
    for i in range(len(tr)):
        td_list = tr[i].find_all('td')
        for j in range(len(td_list)):
            # if td_list[j].get('width') == '0':
            #     print(type(td_list[j].get('width')))
            #     print(td_list[j].get('width'))
            #     lists[i].pop()
            if td_list[j].get('x:num') is None or td_list[j].get('x:num') == '':
                lists[i][j] = td_list[j].get_text()
                # print(lists[i][j])
            else:
                lists[i][j] = td_list[j].get('x:num')
                # print(lists[i][j])
    print(lists)
    return lists


def check_frame(html, m):
    """
        通过正则表达式匹配网页中的frame中的src属性然后拼接成一个url，并将他们保存在一个列表中
        :param html: 已经download好的HTML文本
        :param m: 传入的年份
        :return: 含有所有frame中src属性的连接的链表
        """
    main_frame = re.findall('.*?<frame .*?src="(.*?)" .*?>', html, re.S | re.M)
    frame_list = []
    for i in range(len(main_frame)):
        frame_list.append('http://www.stats-sh.gov.cn/tjnj/%stjnj/%s' % (m, main_frame[i]))
    print(len(frame_list))
    return frame_list


if __name__ == '__main__':
    """
        对所有年份的年鉴进行遍历，通过4种处理方法来应对所有年鉴中出现的4种情况进行处理拼接表格。
        最后将连接超时的url或者处理异常的url捕获，写进文本文档当中。
        第一层while的k变量是年份，对每一年的年鉴
        第二层while的i变量是对应章节年鉴的爬取，为了能保存4位数包括0开头的，用了这fill()这个方法处理了一下
            针对每一个章节的年鉴，都会调用download()函数下载HTML，返回的HTML都会调用check_frame（）函数处理，返回该HTML中所含有的frame的url列表
            针对每一个url列表，应对4中情况：分别是有一个frame都没有；有两个frame；有三个frame；有多于5个的frame
            每一个frame都通过create_table()抓取数据最后将其转成Dataframe，然后再通过pandas把每一块合并成一个表格，最后用表名作为文件名并以csv的形式保存
        """
    k = 2016
    while k > 2004:
            m = str(k)
            i = 101
            while i < 2510:
                try:
                    n = str(i).zfill(4)
                    path = 'http://www.stats-sh.gov.cn/tjnj/%stjnj/C%s.htm' % (m, n)
                    print(path)
                    html, state = download(path, k, headers=headers)
                    print(state)
                    if 400 <= state < 600:
                        i = i + 100 - i % 100 + 1
                        continue
                    frame_list = check_frame(html, m)
                    if len(frame_list) == 0:
                        print('正在处理...' + path)
                        table_lists = create_table(html)
                        title = str(table_lists[0][0]).replace('\n', '').replace('\r', '')
                        body = pd.DataFrame(table_lists)
                        body.to_csv('Shanghai%s%s.csv' % (m, title))
                        i += 1
                    elif len(frame_list) == 2:
                        all_table = []
                        all_data_frame = []
                        title = ''
                        for j in range(len(frame_list)):
                            print('正在处理...' + frame_list[j])
                            html_text, _ = download(frame_list[j], k, headers=headers)
                            table_list = create_table(html_text)
                            all_table.append(table_list)
                            if j == 0:
                                title = str(table_list[0][0]).replace('\n', '').replace('\r', '')
                        for j in range(len(all_table)):
                            all_data_frame.append(pd.DataFrame(all_table[j]))
                        body = pd.concat([all_data_frame[0], all_data_frame[1]])
                        body.to_csv('Shanghai%s%s.csv' % (m, title))
                        i += 1
                    elif len(frame_list) == 3:
                        all_table = []
                        all_data_frame = []
                        title = ''
                        for j in range(len(frame_list)):
                            print('正在处理...' + frame_list[j])
                            html_text, _ = download(frame_list[j], k, headers=headers)
                            table_list = create_table(html_text)
                            all_table.append(table_list)
                            if j == 0:
                                title = str(table_list[0][0]).replace('\n', '').replace('\r', '')
                        for j in range(len(all_table)):
                            all_data_frame.append(pd.DataFrame(all_table[j]))
                        body = pd.concat([all_data_frame[1], all_data_frame[2]])
                        body.to_csv('Shanghai%s%s.csv' % (m, title))
                        i += 1
                    elif len(frame_list) >= 5:
                        all_table = []
                        all_data_frame = []
                        title = ''
                        for j in range(len(frame_list)):
                            print('正在处理...' + frame_list[j])
                            html_text, list_state = download(frame_list[j], k, headers=headers)
                            if 400 <= list_state < 600:
                                continue
                            # print(html_text)
                            if create_table(html_text) is None:
                                continue
                            table_list = create_table(html_text)
                            all_table.append(table_list)
                            if j == 0:
                                title = str(table_list[0][0]).replace('\n', '').replace('\r', '')
                        for j in range(len(all_table)):
                            all_data_frame.append(pd.DataFrame(all_table[j]))
                        print(all_table)
                        center = pd.concat([all_data_frame[1], all_data_frame[2]], axis=1)
                        bottom = pd.concat([all_data_frame[3], all_data_frame[4]], axis=1)
                        body = pd.concat([center, bottom])
                        body.to_csv('Shanghai%s%s.csv' % (m, title))
                        i += 1
                    else:
                        f = open('ExceptionUrl_test.txt', 'a')
                        f.write(path)
                        i += 1
                except:
                    f = open('ExceptionUrl_test.txt', 'a')
                    f.write(path)
                    i += 1
            k -= 1
