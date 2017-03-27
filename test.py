import re

import requests
import pandas as pd
import time
from bs4 import BeautifulSoup

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'}


def download(url, headers):
    try:
        html = requests.get(url, headers=headers, timeout=30)
        time.sleep(2)
        html.encoding = 'gbk'
        state = html.status_code
        # print(html.text)
    except:
        print('链接超时')
        html = None
    return html.text, state


def create_table(html):
    soup = BeautifulSoup(html, 'lxml')
    table = soup.find('table')
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
            if td_list[j].get('x:num') is None or td_list[j].get('x:num') == '':
                lists[i][j] = td_list[j].get_text()
                # print(lists[i][j])
            else:
                lists[i][j] = td_list[j].get('x:num')
                # print(lists[i][j])
    print(lists)
    return lists


def check_frame(html):
    main_frame = re.findall('.*?<frame .*?src="(.*?)" .*?>', html, re.S | re.M)
    frame_list = []
    for i in range(len(main_frame)):
        frame_list.append('http://www.stats-sh.gov.cn/tjnj/2014tjnj/%s' % main_frame[i])
    print(len(frame_list))
    return frame_list


if __name__ == '__main__':
    i = 307
    while i < 2404:
        n = str(i).zfill(4)
        path = 'http://www.stats-sh.gov.cn/tjnj/2014tjnj/C%s.htm' % n
        print(path)
        html, state = download(path, headers=headers)
        print(state)
        if 400 <= state < 600:
            i = i + 100 - i % 100 + 1
            continue
        frame_list = check_frame(html)
        if len(frame_list) == 0:
            print('正在处理...' + path)
            table_lists = create_table(html)
            body = pd.DataFrame(table_lists)
            body.to_csv('Shanghai2014C%s.csv' % n)
            i += 1
        if len(frame_list) == 2:
            all_table = []
            all_data_frame = []
            for j in range(len(frame_list)):
                print('正在处理...' + frame_list[j])
                html_text, _ = download(frame_list[j], headers=headers)
                all_table.append(create_table(html_text))
            for j in range(len(all_table)):
                all_data_frame.append(pd.DataFrame(all_table[j]))
            body = pd.concat([all_data_frame[0], all_data_frame[1]])
            body.to_csv('Shanghai2014C%s.csv' % n)
            i += 1
        if len(frame_list) == 6:
            all_table = []
            all_data_frame = []
            for frame_html in frame_list:
                print('正在处理...' + frame_html)
                html_text, list_state = download(frame_html, headers=headers)
                if 400 <= list_state < 600:
                    continue
                # print(html_text)
                all_table.append(create_table(html_text))
            for j in range(len(all_table)):
                all_data_frame.append(pd.DataFrame(all_table[j]))
            center = pd.concat([all_data_frame[1], all_data_frame[2]], axis=1)
            bottom = pd.concat([all_data_frame[3], all_data_frame[4]], axis=1)
            body = pd.concat([center, bottom])
            body.to_csv('Shanghai2014C%s.csv' % n)
            i += 1