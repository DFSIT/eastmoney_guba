import time
import requests
from bs4 import BeautifulSoup
import re
import aiohttp
import asyncio
import csv
import os
import datetime

def Onlytime(s):
    fomart = '0123456789-:'
    for c in s:
        if not c in fomart:
            s = s.replace(c,'')
    return s

def sort_content_by_date(content):
    content.sort(key=lambda x:datetime.datetime.strptime(x[0],'%Y-%m-%d %H:%M:%S'))

def sort_content_by_idcode(content):
    content.sort(key=lambda x:x[1])

def drop_abnormal_date(content,datetime_date):
    return_list=[]
    today_date_string=datetime_date.strftime('%Y-%m-%d')
    yesterday_date=(datetime_date-datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    tomorrow_data=(datetime_date+datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    for item in content:
        if item[0][:10] in [yesterday_date,today_date_string,tomorrow_data]:
            return_list.append(item)
    return return_list

def sep_content_today_tomorrow(content,date_today_string):
    return_content_today=[]
    return_content_tomorrow=[]
    for item in content:
        if item[0][:10]==date_today_string:
            return_content_today.append(item)
        else:
            return_content_tomorrow.append(item)
    return [return_content_today,return_content_tomorrow]

def sep(start,total,step):
    a=[]
    x,y= divmod(total,step)
    for i in range (x):
       a.append((start+i*step,step))
    if y!=0:
        a.append((start+(x)*step,y))
    return a

def get_homepage_id(sleep_time):
    print('tring to get homepage_id')
    while True:
        page=requests.get('http://guba.eastmoney.com/default,f_1.html')
        s=BeautifulSoup(page.text,"html.parser")
        ul_tag=s.find('ul', class_="newlist")
        settop_count=len(ul_tag.find_all('em', class_="settop"))
        a_tag_list=ul_tag.find_all('a',class_="note")
        total_count=len(a_tag_list)
        if settop_count!=total_count:
            string=a_tag_list[-(total_count-settop_count)]['href']
            homepage_code=int(re.findall(pattern=re.compile("/news,.*?,(\d*?)\.html"),string=string)[0])
            print('got homepage_id: %s'%homepage_code)
            return homepage_code
        time.sleep(sleep_time)

def save_csv(path,data):
    with open(path,'a', newline='',encoding='utf-8') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter='|')
        for item in data:
            spamwriter.writerow(item)


def save_csv_w(path,data):
    with open(path,'w', newline='',encoding='utf-8') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter='|')
        for item in data:
            spamwriter.writerow(item)


def get_post_vol(content):
    if len(content)==1:
        return 0
    else:
        t1=datetime.datetime.strptime(content[0][0],'%Y-%m-%d %H:%M:%S')
        t2=datetime.datetime.strptime(content[-1][0],'%Y-%m-%d %H:%M:%S')
        seconds=(t2-t1).seconds
        # print(seconds)
        post_count=int(content[-1][1])-int(content[0][1])
        post_vol=post_count/seconds
    return post_vol

def read_csv(path):
    return_list=[]
    with open(path,'r',encoding='utf-8',newline='') as csvfile:
        spamreader=csv.reader(csvfile,delimiter='|')
        for line in spamreader:
            return_list.append(line)
    return return_list

def get_max_id(path):
    content_list=read_csv(path)
    content_list.sort(key=lambda x:x[0])
    return int(content_list[-1][0]),datetime.datetime.strptime(content_list[-1][1][:10],'%Y-%m-%d').date()

def get_start_id_date(path,start_id):
    url='http://guba.eastmoney.com/news,0,%s.html'%start_id
    page=requests.get(url)
    s=BeautifulSoup(page.text,'html.parser')
    tag=s.find('div',id="zwcontent")
    post_time_raw=tag.find('div', class_="zwfbtime").get_text()
    #time format  yyyy-mm-ddhh:mm:ss
    post_time_NS=Onlytime(post_time_raw)
    post_time=post_time_NS[:10]


    return_datetime_date_html=datetime.datetime.strptime(post_time,'%Y-%m-%d').date()

    path_list=os.listdir(path)
    path_list.sort(key=lambda x:datetime.datetime.strptime(x,'%Y-%m-%d'))
    path_list.reverse()
    doc_count=len(path_list)
    print(path_list)
    if doc_count==0:
        return start_id,return_datetime_date_html

    else:
        for path_second in path_list:
            try:
                if os.path.getsize(path+path_second+'/'+path_second+'_refer_info.csv'):
                    hist_code,hist_datetime_date=get_max_id(path+path_second+'/'+path_second+'_refer_info.csv')
                    print(hist_code)
                    if hist_code>start_id:
                        return hist_code,hist_datetime_date
                    else:
                        return start_id,return_datetime_date_html
            except Exception:
                pass
        return start_id,return_datetime_date_html


class Async_infi_Spider():
    def __init__(self, idcode,datetime_date, concurrency,max_concurrency=4000,num_per=500):
        self.concurrency=concurrency
        self.input_start_code=idcode
        self.max_concurrency=max_concurrency
        self.num_per=num_per

        self.url='http://guba.eastmoney.com/news,0,%s.html'

        self.return_count=0
        self.reached_max_code=self.input_start_code
        self.content=[]
        self.content_temp=[]
        self.save_content_today_tomorrow=[]
        self.save_content_today=[]
        self.save_content_tomorrow=[]
        self.homepage_id=idcode
        self.concurrency_temp_step=10


        self.post_vol=1
        self.post_count=1
        self.post_seconds=1

        self.path='./guba_crawled_data/'

        '''当前抓取的这一天 非系统时间'''
        self.date_today_datetime=datetime_date
        self.date_today_string=self.date_today_datetime.strftime('%Y-%m-%d')
        self.date_tomorrow_datetime=self.date_today_datetime+datetime.timedelta(days=1)
        self.date_tomorrow_string=self.date_tomorrow_datetime.strftime('%Y-%m-%d')
        self.csv_saving_path_today=self.path+self.date_today_string+'/'+self.date_today_string+'.csv'
        self.log_saving_path_today=self.path+self.date_today_string+'/'+self.date_today_string+'_log.csv'



        self.sleep_flag=1

        self.refer_info_saving_path=self.path+self.date_today_string+'/'+self.date_today_string+'_refer_info.csv'





    @asyncio.coroutine
    def fetch(self,url,idcode):
        try:
            resp = yield from aiohttp.request('GET', url, allow_redirects=False)
            if resp.status==200:
                html = yield from resp.text()
                resp.close()
                self.handle_html(html,idcode)
            else:
                resp.close()
        except Exception as e:
            with open(self.log_saving_path_today,'a', newline='',encoding='utf-8') as csvfile:
                spamwriter=csv.writer(csvfile, delimiter='|')
                spamwriter.writerow([idcode,e.__str__()])
            print(e)


    def work(self, start_code, concurrency):
        loop = asyncio.get_event_loop()
        f = asyncio.wait([self.fetch(url,idcode) for url,idcode in [(self.url%(i+start_code),i+start_code) for i in range(concurrency)]])
        loop.run_until_complete(f)


    def handle_html(self,html,idcode):
        try:
            self.return_count+=1
            self.reached_max_code=max(self.reached_max_code,idcode)
            s=BeautifulSoup(html,"html.parser")
            bar=s.find('span',id="stockname").get_text()
            idcode=idcode

            tag=s.find('div',id="zwcontent")
            post_time_raw=tag.find('div', class_="zwfbtime").get_text()
            #time format  yyyy-mm-ddhh:mm:ss
            post_time_NS=Onlytime(post_time_raw)
            post_time=post_time_NS[:10]+' '+post_time_NS[10:]
            title=tag.find('div',id="zwconttbt").get_text()
            author=tag.find('div',id="zwconttbn").get_text()
            article_content=tag.find('div',class_="stockcodec").get_text()
            self.content.append([post_time,idcode,bar,title,author,article_content])
        except Exception as e:
            print(str(idcode),e,'in handle')


    def _run(self):
        while 1:
            t1=time.time()
            for i in sep(self.input_start_code,self.concurrency,self.num_per):
                print('sending %s requests'%i[1],self.input_start_code,self.concurrency,self.num_per)
                self.work(i[0],i[1])

            if not os.path.exists(self.path+self.date_today_string+'/'):
                print('未发现路径 创建路径中 %s'%(self.path+self.date_today_string+'/'))
                os.mkdir(self.path+self.date_today_string+'/')


            if self.return_count==0:
                print('没有返回数据')
                self.homepage_id=get_homepage_id(1)
                if self.homepage_id>self.input_start_code+self.concurrency-1:
                    print('主页已刷新',self.homepage_id)
                    self.input_start_code=self.input_start_code+self.concurrency
                    self.concurrency_temp_step=self.homepage_id-self.input_start_code+1
                else:
                    print('主页未刷新')
                continue

            elif self.return_count!=0:

                '''先排序 再删除异常值'''
                print('获取 %s 条内容 整理中'%self.return_count)
                sort_content_by_date(self.content)
                self.content_temp=drop_abnormal_date(self.content,self.date_today_datetime)
                self.content=self.content_temp
                self.content_temp=[]


                '''删除后若 content 不为空'''
                if len(self.content):
                    '''取速度参数'''
                    self.post_vol=get_post_vol(self.content)
                    '''取最新爬帖的发表时间'''
                    self.last_post_datetime=datetime.datetime.strptime(self.content[-1][0],'%Y-%m-%d %H:%M:%S')
                    print('获取发帖速度参数和最后发帖时间...','%.2f'%self.post_vol,self.last_post_datetime)
                    '''判断是否完成当天任务'''
                    if self.content[-1][0][:10]==self.date_tomorrow_string:
                        print('到达时间分隔')
                        '''进入下一天 储存今天的数据'''
                        self.save_content_today_tomorrow=sep_content_today_tomorrow(self.content,self.date_today_string)
                        self.save_content_today=self.save_content_today_tomorrow[0]
                        self.save_content_tomorrow=self.save_content_today_tomorrow[1]
                        self.save_content_today_tomorrow=[]

                        self.content=self.save_content_tomorrow
                        self.save_content_tomorrow=[]
                        save_csv(self.csv_saving_path_today,self.save_content_today)
                        sort_content_by_idcode(self.save_content_today)
                        self.refer_info_next_start=[self.save_content_today[-1][1],self.save_content_today[-1][0]]
                        self.save_content_today=[]
                        print('数据存储完毕')
                        # '''数据整理'''
                        #
                        # temp_tank_list=read_csv(self.csv_saving_path_today)
                        # sort_content_by_date(temp_tank_list)
                        # save_csv(self.csv_saving_path_today,temp_tank_list)
                        # del temp_tank_list
                        # print('数据整理完毕')
                        '''重设数据'''
                        self.date_today_datetime+=datetime.timedelta(days=1)
                        self.date_today_string=self.date_today_datetime.strftime('%Y-%m-%d')
                        self.date_tomorrow_datetime=self.date_today_datetime+datetime.timedelta(days=1)
                        self.date_tomorrow_string=self.date_tomorrow_datetime.strftime('%Y-%m-%d')
                        self.csv_saving_path_today=self.path+self.date_today_string+'/'+self.date_today_string+'.csv'
                        self.log_saving_path_today=self.path+self.date_today_string+'/'+self.date_today_string+'_log.txt'


                    elif self.content[-1][0][:10]==self.date_today_string:
                        self.refer_info_next_start=[self.content[-1][1],self.content[-1][0]]
                        save_csv(self.csv_saving_path_today,self.content)
                        self.content=[]
                        print('当天数据储存完毕')
                '''修改起始点'''
                self.input_start_code=self.reached_max_code
                '''数据储存完毕 查看是否需要修改 爬行速度'''
                if datetime.datetime.now()-self.last_post_datetime>datetime.timedelta(minutes=30):
                    self.concurrency=self.max_concurrency
                    self.sleep_flag=0
                    print('并发速度为最大限速 %s'%self.max_concurrency)
                elif datetime.datetime.now()-self.last_post_datetime<=datetime.timedelta(minutes=30):
                    self.sleep_flag=1
                    if self.post_vol==0:
                        self.concurrency=max(self.concurrency,self.concurrency_temp_step)
                        print('未获得发帖速度 %s'%self.concurrency)
                    else:
                        seconds=abs((datetime.datetime.now()-self.last_post_datetime).seconds)
                        multi_para=max(0,0.0127*seconds-2.8)
                        self.concurrency=min(self.max_concurrency,max(int(multi_para*self.post_vol*60),self.concurrency_temp_step))
                        print('调整并发速度 %s'%self.concurrency)
                self.return_count=0
                self.concurrency_temp_step=10
                '''记录 本次爬行最大id 和 时间'''
                save_csv(self.refer_info_saving_path,[self.refer_info_next_start])
            t2=time.time()
            print('time cost %.2f'%(t2-t1))
            if t2-t1<60 and t2-t1>0 and self.sleep_flag==1:
                print('sleeping...')
                time.sleep(60-(t2-t1))
            print('\n')


def main(start_id,requests_amount,max_amount,requests_per_time):
    print('初始化')
    print('正在获取  起始id  起始date')
    if not os.path.exists('./guba_crawled_data/'):
        os.mkdir('./guba_crawled_data/')
    start_code,datetime_date=get_start_id_date('./guba_crawled_data/',start_id)
    print('获取完毕')
    print('起始id:%s 起始date:%s 起始并发数:%s'%(start_code,datetime_date,requests_amount))
    print('启动爬虫')
    sever_spider=Async_infi_Spider(start_code,datetime_date,concurrency=requests_amount,max_concurrency=max_amount,num_per=requests_per_time)
    sever_spider._run()
if __name__ == '__main__':
    main(428327780,1000,4000,500)


