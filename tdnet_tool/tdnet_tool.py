import os
import datetime as dt
import pandas as pd
from sys import exit
from bs4 import BeautifulSoup
from urllib.request import urlopen
import time
from sqlite3 import connect
from urllib3 import PoolManager
from requests import post
from zipfile import ZipFile, ZIP_DEFLATED

class tdNet:
    def __init__(self, db_path = 'tdnet.db'):
        self.db_path = db_path
        self.date_base=dt.datetime.now()
        self.date_min=self.date_base-dt.timedelta(days=30)
        self.df = None

    def getData_tdnet_byDay(self,dateYmd=None):
        # 日付取得
        if dateYmd == None:
            date = self.date_base
        else:
            date = dt.datetime.strptime(dateYmd, '%Y-%m-%d')
        
        # 1ページ目のデータ取得
        r = urlopen('https://www.release.tdnet.info/inbs/I_list_{}_{}.html'.format(
            str(1).zfill(3), date.strftime('%Y%m%d')))
        soup = BeautifulSoup(r, 'lxml')
        
        #0件なら終了
        if soup.find(class_='kaijiSum')==None:
            exit()
        
        # 件数取得（1周目の例外処理）
        n_raw = soup.find(class_='kaijiSum').text.split(
            '/')[1].replace('全', '').replace('件', '').replace('\xa0', '')
        n = int(int(n_raw)/100)+1

        # 要素をリスト化
        list_td = []
        url_td = 'https://www.release.tdnet.info/inbs/'
        for i in range(1, n+1):
            # 2ページ目の以降のデータ取得
            if i > 1:
                r = urlopen('https://www.release.tdnet.info/inbs/I_list_{}_{}.html'.format(
                    str(i).zfill(3), date.strftime('%Y%m%d')))
                soup = BeautifulSoup(r, 'lxml')

            for e in soup.find(id="main-list-table").find_all('tr'):
                # XBRLの処理
                xbrl_url = e.find(
                    class_=['oddnew-M kjXbrl', 'evennew-M kjXbrl']).find('a')
                if xbrl_url == None:
                    xbrl = ''
                else:
                    xbrl = url_td + str(xbrl_url.get('href'))

                # 日付の処理
                str_time = e.find(
                    class_=['oddnew-L kjTime', 'evennew-L kjTime']).text

                # その他要素とリストの処理
                list_td.append([dt.datetime.strptime(
                    date.strftime('%Y/%m/%d ')+str_time, '%Y/%m/%d %H:%M'),
                                e.find(class_=['oddnew-M kjCode',
                                            'evennew-M kjCode']).text[0:4],
                                e.find(class_=['oddnew-M kjName',
                                            'evennew-M kjName']).text,
                                e.find(class_=['oddnew-M kjTitle',
                                            'evennew-M kjTitle']).text,
                                url_td +
                                e.find(
                                    class_=['oddnew-M kjTitle', 'evennew-M kjTitle']).
                                    find('a').get('href'),
                                xbrl,
                                e.find(class_=['oddnew-M kjPlace',
                                            'evennew-M kjPlace']).text,
                                e.find(class_=['oddnew-R kjHistroy',
                                            'evennew-R kjHistroy']).text
                                ])
            # 時間調整
            time.sleep(3)

        # ループ後、リストをデータフレーム化
        colname = ['date', 'code', 'name', 'title',
                'pdf', 'xbrl', 'place', 'history']
        df = pd.DataFrame(list_td, columns=colname)

        # 空白処理
        df['name'] = df['name'].str.strip()
        df['place'] = df['place'].str.strip()
        df['history'] = df['history'].str.strip()
        # toSQL(df)
        self.df=df
    
    def getData_tdnet_KeywordSearch(self, keyword, date_start=None, date_end=None):
        if date_end is None:
            # デフォルトは当日
            date_end = self.date_base
        else:
            # Y-m-d -> Ymdへ変換
            date_end = dt.datetime.strptime(date_end, '%Y-%m-%d')

        if date_start is None:
            # デフォルトは30日前
            date_start = self.date_min
        else:
            # 30日以内
            date_start = max(dt.datetime.strptime(
                date_start, '%Y-%m-%d'), self.date_min)
            date_start = min(date_start, date_end)
        
        date_start=date_start.strftime('%Y%m%d')
        date_end=date_end.strftime('%Y%m%d')

        # POST
        data = {
            't0': date_start,  # strftime('%Y%m%d')
            't1': date_end,  # strftime('%Y%m%d')
            'q': keyword,  # unicode
            'm': '0'
        }
        response = post(
            'https://www.release.tdnet.info/onsf/TDJFSearch/TDJFSearch', data=data)
        soup = BeautifulSoup(response.text, 'lxml')

        list_td = []
        url_td = 'https://www.release.tdnet.info'
        for e in soup.find_all(class_=['odd', 'even']):
            if (e.find(class_='xbrl-button')) == None:
                xbrl = ''
            else:
                xbrl = url_td + e.find(class_='xbrl-button').get('href')

            list_td.append([dt.datetime.strptime(e.find(class_='time').text, '%Y/%m/%d %H:%M'),
                            e.find(class_='code').text[0:4],
                            e.find(class_='companyname').text,
                            e.find(class_='title').text,
                            url_td + e.find(class_='title').find('a').get('href'),
                            xbrl,
                            e.find(class_='exchange').text,
                            e.find(class_='update').text])

        colname = ['date', 'code', 'name', 'title',
                'pdf', 'xbrl', 'place', 'history']
        self.df = pd.DataFrame(list_td, columns=colname)

    def toSQL(self):
        if os.path.exists(self.db_path) == False:
            self._createDB()

        conn = connect(self.db_path)
        self.df.to_sql('tdnet_tmp', conn, if_exists='replace', index=None)
        conn.close()
        self._insert_DB()
    
    def _insert_DB(self):
        conn = connect(self.db_path)
        c = conn.cursor()
        c.executescript('INSERT or IGNORE INTO tdnet SELECT * FROM tdnet_tmp')
        conn.close()

    def _createDB(self):
        conn = connect(self.db_path)
        c = conn.cursor()
        c.execute('''CREATE TABLE tdnet
                    (date text, code text, name text, title text, pdf text, xbrl text, place text, history text)''')

    def downloadPDF(self, limit=3):
        try:
            df=self.df.head(limit)
        except:
            df=self.df
        df=df.query('date >= @self.date_min')
        
        with ZipFile('tdnet.zip', 'w', compression=ZIP_DEFLATED) as new_zip:
            for pdf, code, date, title in zip(df['pdf'], df['code'], df['date'], df['title']):
                url = pdf
                filename = str(code) + '_' + str(date)[:10].replace('-', '')+'_'+title+'.pdf'

                #download
                request_methods = PoolManager()
                response = request_methods.request('GET', url)
                f = open(filename, 'wb')
                f.write(response.data)
                f.close()
                new_zip.write(filename)
                os.remove(filename)
                
                # 時間調整
                time.sleep(3)

    def getData_SQL(self, strSQL):
        conn = connect(self.db_path)
        self.df = pd.read_sql_query(strSQL, conn)
        #self.df['date'] =self.df['date'].apply(lambda x:pd.to_datetime(x,format='%Y-%m-%d %H:%M:%S'))
        self.df['date'] =self.df['date'].apply(pd.to_datetime)

    def toHTML(self,filename='tdnet_list.html'):
        self.df.to_html(filename, render_links=True)
    
    def toCSV(self,filename='tdnet_list.csv',encoding='utf_8_sig'):
        self.df.to_csv(filename,encoding=encoding)
