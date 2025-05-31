#!/usr/bin/env python3
# coding: utf-8
import os
from time import sleep
import datetime as dt
import pandas as pd
from sys import exit
from bs4 import BeautifulSoup
from urllib.request import urlopen,Request,ProxyHandler,build_opener,install_opener
from urllib.parse import urlencode
from sqlite3 import connect
# from urllib3 import PoolManager
# from requests import post
from zipfile import ZipFile, ZIP_DEFLATED


class tdNet:
    def __init__(self, db_path="tdnet.db",proxy=None):
        self.db_path = db_path
        self.date_base = dt.datetime.now()
        self.date_min = self.date_base - dt.timedelta(days=30)
        self.df = None
        if proxy is not None:
            self.opener = build_opener(ProxyHandler(proxy))
            install_opener(self.opener)
        else:
            self.opener = build_opener()
            install_opener(self.opener)

    def getData_tdnet_byDay(self, dateYmd=None):
        # 日付取得
        if dateYmd == None:
            date = self.date_base
        else:
            date = dt.datetime.strptime(dateYmd, "%Y-%m-%d")

        # 1ページ目のデータ取得
        r = urlopen(
            "https://www.release.tdnet.info/inbs/I_list_{}_{}.html".format(
                str(1).zfill(3), date.strftime("%Y%m%d")
            )
        )
        soup = BeautifulSoup(r, "lxml")

        # 0件なら終了
        if soup.find(class_="kaijiSum") == None:
            exit()

        # 件数取得（1周目の例外処理）
        n_raw = (
            soup.find(class_="kaijiSum")
            .text.split("/")[1]
            .replace("全", "")
            .replace("件", "")
            .replace("\xa0", "")
        )
        n = int(int(n_raw) / 100) + 1

        # 要素をリスト化
        list_td = []
        url_td = "https://www.release.tdnet.info/inbs/"
        for i in range(1, n + 1):
            # 2ページ目の以降のデータ取得
            if i > 1:
                r = urlopen(
                    "https://www.release.tdnet.info/inbs/I_list_{}_{}.html".format(
                        str(i).zfill(3), date.strftime("%Y%m%d")
                    )
                )
                soup = BeautifulSoup(r, "lxml")

            for e in soup.find(id="main-list-table").find_all("tr"):
                # hrefの処理, なければループスキップ
                try:
                    href = url_td + e.find(
                        class_=["oddnew-M kjTitle", "evennew-M kjTitle"]
                    ).find("a").get("href")
                except AttributeError:
                    continue

                # XBRLの処理
                xbrl_url = e.find(class_=["oddnew-M kjXbrl", "evennew-M kjXbrl"]).find(
                    "a"
                )
                if xbrl_url == None:
                    xbrl = ""
                else:
                    xbrl = url_td + str(xbrl_url.get("href"))

                # 日付の処理
                str_time = e.find(class_=["oddnew-L kjTime", "evennew-L kjTime"]).text

                # その他要素とリストの処理
                list_td.append(
                    [
                        dt.datetime.strptime(
                            date.strftime("%Y/%m/%d ") + str_time, "%Y/%m/%d %H:%M"
                        ),
                        e.find(class_=["oddnew-M kjCode", "evennew-M kjCode"]).text[
                            0:4
                        ],
                        e.find(class_=["oddnew-M kjName", "evennew-M kjName"]).text,
                        e.find(class_=["oddnew-M kjTitle", "evennew-M kjTitle"]).text,
                        href,
                        xbrl,
                        e.find(class_=["oddnew-M kjPlace", "evennew-M kjPlace"]).text,
                        e.find(
                            class_=["oddnew-R kjHistroy", "evennew-R kjHistroy"]
                        ).text,
                    ]
                )
            # 時間調整
            sleep(2)

        # ループ後、リストをデータフレーム化
        colname = [
            "datetime",
            "code",
            "name",
            "title",
            "pdf",
            "xbrl",
            "place",
            "history",
        ]
        df = pd.DataFrame(list_td, columns=colname)

        # 空白処理
        df["name"] = df["name"].str.strip()
        df["place"] = df["place"].str.strip()
        df["history"] = df["history"].str.strip()
        df["date"] = df["datetime"].dt.strftime("%Y-%m-%d")
        df["time"] = df["datetime"].dt.strftime("%H:%M:%S")
        # toSQL(df)
        self.df = df

    def getData_tdnet_KeywordSearch(self, keyword, date_start=None, date_end=None):
        if date_end is None:
            # デフォルトは当日
            date_end = self.date_base
        else:
            # Y-m-d -> Ymdへ変換
            date_end = dt.datetime.strptime(date_end, "%Y-%m-%d")

        if date_start is None:
            # デフォルトは30日前
            date_start = self.date_min
        else:
            # 30日以内
            date_start = max(
                dt.datetime.strptime(date_start, "%Y-%m-%d"), self.date_min
            )
            date_start = min(date_start, date_end)

        date_start = date_start.strftime("%Y%m%d")
        date_end = date_end.strftime("%Y%m%d")

        # POST
        data = {
            "t0": date_start,  # strftime('%Y%m%d')
            "t1": date_end,  # strftime('%Y%m%d')
            "q": keyword,  # unicode
            "m": "0",
        }
        # response = post(
        #     "https://www.release.tdnet.info/onsf/TDJFSearch/TDJFSearch", data=data
        # )
        # soup = BeautifulSoup(response.text, "lxml")
        response = Request(
            "https://www.release.tdnet.info/onsf/TDJFSearch/TDJFSearch", data=urlencode(data).encode('utf-8')
        )
        response = self.opener.open(response)
        soup = BeautifulSoup(response, "lxml")

        list_td = []
        url_td = "https://www.release.tdnet.info"
        for e in soup.find_all(class_=["odd", "even"]):
            if (e.find(class_="xbrl-button")) == None:
                xbrl = ""
            else:
                xbrl = url_td + e.find(class_="xbrl-button").get("href")

            list_td.append(
                [
                    dt.datetime.strptime(e.find(class_="time").text, "%Y/%m/%d %H:%M"),
                    e.find(class_="code").text[0:4],
                    e.find(class_="companyname").text,
                    e.find(class_="title").text,
                    url_td + e.find(class_="title").find("a").get("href"),
                    xbrl,
                    e.find(class_="exchange").text,
                    e.find(class_="update").text,
                ]
            )

        colname = [
            "datetime",
            "code",
            "name",
            "title",
            "pdf",
            "xbrl",
            "place",
            "history",
        ]
        df = pd.DataFrame(list_td, columns=colname)
        if len(df) > 0:
            df["date"] = df["datetime"].dt.strftime("%Y-%m-%d")
            df["time"] = df["datetime"].dt.strftime("%H:%M:%S")
        self.df = df

    def toSQL(self):
        if os.path.exists(self.db_path) == False:
            self._createDB()

        conn = connect(self.db_path)
        self.df.to_sql("tdnet_tmp", conn, if_exists="replace", index=None)
        conn.close()
        self._insert_DB()

    def _insert_DB(self):
        conn = connect(self.db_path)
        c = conn.cursor()
        c.executescript("INSERT or IGNORE INTO tdnet SELECT * FROM tdnet_tmp")
        conn.close()

    def _createDB(self):
        conn = connect(self.db_path)
        c = conn.cursor()
        c.execute(
            """CREATE TABLE tdnet
                    (datetime text, code text, name text, title text, pdf text, xbrl text, place text, history text, date text, time text, UNIQUE(pdf))"""
        )

    def downloadPDF(self, filename="tdnet.zip", limit=3):
        try:
            df = self.df.head(limit)
        except:
            df = self.df
        df = df[df["datetime"] > self.date_min]

        with ZipFile(filename, "w", compression=ZIP_DEFLATED) as new_zip:
            for pdf, code, date, title in zip(
                df["pdf"], df["code"], df["date"], df["title"]
            ):
                url = pdf
                filename = (
                    str(code)
                    + "_"
                    + str(date)[:10].replace("-", "")
                    + "_"
                    + title
                    + ".pdf"
                )

                # download
                # request_methods = PoolManager()
                # response = request_methods.request("GET", url)
                response = self.opener.open(url)
                f = open(filename, "wb")
                f.write(response.data)
                f.close()
                new_zip.write(filename)
                os.remove(filename)

                # 時間調整
                sleep(3)

    def getData_SQL(self, strSQL, astype_datetime=True):
        conn = connect(self.db_path)
        self.df = pd.read_sql_query(strSQL, conn)
        # self.df['date'] =self.df['date'].apply(lambda x:pd.to_datetime(x,format='%Y-%m-%d %H:%M:%S'))
        if astype_datetime == True:
            self.df["datetime"] = self.df["datetime"].apply(pd.to_datetime)

    def toHTML(self, filename="tdnet_list.html", encoding="utf_8_sig"):
        self.df.to_html(filename, render_links=True, encoding=encoding)

    def toCSV(self, filename="tdnet_list.csv", encoding="utf_8_sig"):
        if self.df.size:
            # 既存のCSVファイルが存在する場合は読み込む
            if os.path.exists(filename):
                existing_df = pd.read_csv(filename, encoding=encoding)
                # 新しいデータと既存のデータを結合し、重複を削除
                combined_df = pd.concat([existing_df, self.df]).drop_duplicates(
                    subset=["pdf"]
                )
                combined_df.to_csv(filename, index=False, encoding=encoding)
            else:
                # ファイルが存在しない場合は新規作成
                self.df.to_csv(filename, index=False, encoding=encoding)


def xbrl_to_csv(xbrl_paths):
    # 全ファイルのデータを格納するリスト
    all_xbrl_data = []

    # 各ファイルを処理
    for file_type, xbrl_path in xbrl_paths.items():
        # XBRLファイルから要素を抽出
        with open(xbrl_path, "r", encoding="utf-8") as f:
            xbrl_content = f.read()

        # BeautifulSoupでパース
        soup = BeautifulSoup(xbrl_content, "html.parser")

        # 必要な要素を抽出
        elements = soup.find_all(["ix:nonnumeric", "ix:nonfraction"])

        # 各要素から必要な属性を抽出
        for element in elements:
            pname, name = element.get("name", "").split(":")
            data = {
                "file_type": file_type,  # dictのkeyを使用
                "contextRef": element.get("contextref", ""),
                "name_prefix": pname,
                "name": name,
                # 'name': element.get('name', ''),
                "format": element.get("format", ""),
                "unitRef": element.get("unitref", ""),
                "decimals": element.get("decimals", ""),
                "scale": element.get("scale", ""),
                "sign": element.get("sign", ""),
                "text": element.text.strip(),
            }
            all_xbrl_data.append(data)

    # DataFrameに変換
    df_xbrl = pd.DataFrame(all_xbrl_data)
    # df_xbrl[['name_prefix','name']] = df_xbrl['name'].str.split(':', expand=True)
    df_xbrl["text"] = df_xbrl.apply(
        lambda row: (
            (
                pd.to_numeric(row["text"].replace(",", ""), errors="coerce")
                * (-1 if row["sign"] == "-" else 1)
            )
            if len(row["scale"]) > 0
            else row["text"]
        ),
        axis=1,
    )
    return df_xbrl