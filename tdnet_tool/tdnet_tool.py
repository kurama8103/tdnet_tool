#!/usr/bin/env python3
# coding: utf-8
import os
import datetime as dt
import pandas as pd
from sys import exit
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request, ProxyHandler, build_opener, install_opener
from urllib.parse import urlencode
from sqlite3 import connect
from zipfile import ZipFile, ZIP_DEFLATED
import logging
from typing import Optional, Dict, List, Union
import time

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TdNetError(Exception):
    """TDnet関連のエラーを扱うカスタム例外クラス"""
    pass

class tdNet:
    """TDnetからデータを取得・処理するクラス"""
    
    BASE_URL = "https://www.release.tdnet.info"
    SEARCH_URL = f"{BASE_URL}/onsf/TDJFSearch/TDJFSearch"
    LIST_URL = f"{BASE_URL}/inbs/I_list_{{}}_{{}}.html"
    
    def __init__(self, db_path: str = "tdnet.db", proxy: Optional[Dict] = None):
        """
        初期化
        
        Args:
            db_path (str): データベースファイルのパス
            proxy (Optional[Dict]): プロキシ設定
        """
        self.db_path = db_path
        self.date_base = dt.datetime.now()
        self.date_min = self.date_base - dt.timedelta(days=30)
        self.df = None
        
        # プロキシ設定
        if proxy is not None:
            self.opener = build_opener(ProxyHandler(proxy))
        else:
            self.opener = build_opener()
        install_opener(self.opener)

    def _make_request(self, url: str, data: Optional[Dict] = None) -> BeautifulSoup:
        """
        HTTPリクエストを実行し、BeautifulSoupオブジェクトを返す
        
        Args:
            url (str): リクエスト先URL
            data (Optional[Dict]): POSTデータ
            
        Returns:
            BeautifulSoup: パースされたHTML
            
        Raises:
            TdNetError: リクエスト失敗時
        """
        try:
            if data:
                request = Request(url, data=urlencode(data).encode('utf-8'))
            else:
                request = Request(url)
            
            response = self.opener.open(request)
            return BeautifulSoup(response, "lxml")
        except Exception as e:
            logger.error(f"リクエスト失敗: {url}, エラー: {str(e)}")
            raise TdNetError(f"リクエスト失敗: {str(e)}")

    def getData_tdnet_byDay(self, dateYmd: Optional[str] = None) -> None:
        """
        指定日のTDnetデータを取得
        
        Args:
            dateYmd (Optional[str]): 日付（YYYY-MM-DD形式）
        """
        try:
            date = dt.datetime.strptime(dateYmd, "%Y-%m-%d") if dateYmd else self.date_base
            
            # 1ページ目のデータ取得
            soup = self._make_request(
                self.LIST_URL.format(str(1).zfill(3), date.strftime("%Y%m%d"))
            )
            
            if soup.find(class_="kaijiSum") is None:
                logger.info("該当データなし")
                return
            
            # 件数取得
            n_raw = (
                soup.find(class_="kaijiSum")
                .text.split("/")[1]
                .replace("全", "")
                .replace("件", "")
                .replace("\xa0", "")
            )
            n = int(int(n_raw) / 100) + 1
            
            # データ取得
            list_td = []
            for i in range(1, n + 1):
                if i > 1:
                    soup = self._make_request(
                        self.LIST_URL.format(str(i).zfill(3), date.strftime("%Y%m%d"))
                    )
                
                for e in soup.find(id="main-list-table").find_all("tr"):
                    try:
                        href = self.BASE_URL + e.find(
                            class_=["oddnew-M kjTitle", "evennew-M kjTitle"]
                        ).find("a").get("href")
                    except AttributeError:
                        continue
                    
                    xbrl_url = e.find(class_=["oddnew-M kjXbrl", "evennew-M kjXbrl"]).find("a")
                    xbrl = self.BASE_URL + str(xbrl_url.get("href")) if xbrl_url else ""
                    
                    str_time = e.find(class_=["oddnew-L kjTime", "evennew-L kjTime"]).text
                    
                    list_td.append([
                        dt.datetime.strptime(
                            date.strftime("%Y/%m/%d ") + str_time, "%Y/%m/%d %H:%M"
                        ),
                        e.find(class_=["oddnew-M kjCode", "evennew-M kjCode"]).text[0:4],
                        e.find(class_=["oddnew-M kjName", "evennew-M kjName"]).text,
                        e.find(class_=["oddnew-M kjTitle", "evennew-M kjTitle"]).text,
                        href,
                        xbrl,
                        e.find(class_=["oddnew-M kjPlace", "evennew-M kjPlace"]).text,
                        e.find(class_=["oddnew-R kjHistroy", "evennew-R kjHistroy"]).text,
                    ])
                
                time.sleep(2)  # サーバー負荷軽減
            
            # データフレーム化
            colname = [
                "datetime", "code", "name", "title", "pdf", "xbrl",
                "place", "history"
            ]
            self.df = pd.DataFrame(list_td, columns=colname)
            
            # データクリーニング
            self.df["name"] = self.df["name"].str.strip()
            self.df["place"] = self.df["place"].str.strip()
            self.df["history"] = self.df["history"].str.strip()
            self.df["date"] = self.df["datetime"].dt.strftime("%Y-%m-%d")
            self.df["time"] = self.df["datetime"].dt.strftime("%H:%M:%S")
            
        except Exception as e:
            logger.error(f"データ取得失敗: {str(e)}")
            raise TdNetError(f"データ取得失敗: {str(e)}")

    def getData_tdnet_KeywordSearch(
        self, keyword: str, date_start: Optional[str] = None, date_end: Optional[str] = None
    ) -> None:
        """
        キーワード検索を実行
        
        Args:
            keyword (str): 検索キーワード
            date_start (Optional[str]): 開始日（YYYY-MM-DD形式）
            date_end (Optional[str]): 終了日（YYYY-MM-DD形式）
        """
        try:
            date_end = (
                dt.datetime.strptime(date_end, "%Y-%m-%d")
                if date_end
                else self.date_base
            )
            
            date_start = (
                max(
                    dt.datetime.strptime(date_start, "%Y-%m-%d"),
                    self.date_min
                )
                if date_start
                else self.date_min
            )
            date_start = min(date_start, date_end)
            
            data = {
                "t0": date_start.strftime("%Y%m%d"),
                "t1": date_end.strftime("%Y%m%d"),
                "q": keyword,
                "m": "0",
            }
            
            soup = self._make_request(self.SEARCH_URL, data)
            
            list_td = []
            for e in soup.find_all(class_=["odd", "even"]):
                xbrl = (
                    self.BASE_URL + e.find(class_="xbrl-button").get("href")
                    if e.find(class_="xbrl-button")
                    else ""
                )
                
                list_td.append([
                    dt.datetime.strptime(e.find(class_="time").text, "%Y/%m/%d %H:%M"),
                    e.find(class_="code").text[0:4],
                    e.find(class_="companyname").text,
                    e.find(class_="title").text,
                    self.BASE_URL + e.find(class_="title").find("a").get("href"),
                    xbrl,
                    e.find(class_="exchange").text,
                    e.find(class_="update").text,
                ])
            
            colname = [
                "datetime", "code", "name", "title", "pdf", "xbrl",
                "place", "history"
            ]
            self.df = pd.DataFrame(list_td, columns=colname)
            
            if len(self.df) > 0:
                self.df["date"] = self.df["datetime"].dt.strftime("%Y-%m-%d")
                self.df["time"] = self.df["datetime"].dt.strftime("%H:%M:%S")
                
        except Exception as e:
            logger.error(f"キーワード検索失敗: {str(e)}")
            raise TdNetError(f"キーワード検索失敗: {str(e)}")

    def toSQL(self) -> None:
        """データをSQLiteデータベースに保存"""
        try:
            if not os.path.exists(self.db_path):
                self._createDB()
            
            conn = connect(self.db_path)
            self.df.to_sql("tdnet_tmp", conn, if_exists="replace", index=None)
            conn.close()
            self._insert_DB()
            
        except Exception as e:
            logger.error(f"SQL保存失敗: {str(e)}")
            raise TdNetError(f"SQL保存失敗: {str(e)}")

    def _insert_DB(self) -> None:
        """一時テーブルからメインテーブルにデータを挿入"""
        try:
            conn = connect(self.db_path)
            c = conn.cursor()
            c.executescript("INSERT or IGNORE INTO tdnet SELECT * FROM tdnet_tmp")
            conn.close()
        except Exception as e:
            logger.error(f"データ挿入失敗: {str(e)}")
            raise TdNetError(f"データ挿入失敗: {str(e)}")

    def _createDB(self) -> None:
        """データベースとテーブルを作成"""
        try:
            conn = connect(self.db_path)
            c = conn.cursor()
            c.execute(
                """CREATE TABLE tdnet
                    (datetime text, code text, name text, title text,
                     pdf text, xbrl text, place text, history text,
                     date text, time text, UNIQUE(pdf))"""
            )
            conn.close()
        except Exception as e:
            logger.error(f"データベース作成失敗: {str(e)}")
            raise TdNetError(f"データベース作成失敗: {str(e)}")

    def downloadPDF(self, filename: str = "tdnet.zip", limit: int = 3) -> None:
        """
        PDFファイルをダウンロードしてZIPに保存
        
        Args:
            filename (str): 出力ZIPファイル名
            limit (int): ダウンロードするファイル数の上限
        """
        try:
            df = self.df.head(limit) if self.df is not None else pd.DataFrame()
            df = df[df["datetime"] > self.date_min]
            
            with ZipFile(filename, "w", compression=ZIP_DEFLATED) as new_zip:
                for pdf, code, date, title in zip(
                    df["pdf"], df["code"], df["date"], df["title"]
                ):
                    filename = (
                        f"{code}_{date[:10].replace('-', '')}_{title}.pdf"
                    )
                    
                    response = self.opener.open(pdf)
                    with open(filename, "wb") as f:
                        f.write(response.read())
                    
                    new_zip.write(filename)
                    os.remove(filename)
                    
                    time.sleep(3)  # サーバー負荷軽減
                    
        except Exception as e:
            logger.error(f"PDFダウンロード失敗: {str(e)}")
            raise TdNetError(f"PDFダウンロード失敗: {str(e)}")

    def getData_SQL(self, strSQL: str, astype_datetime: bool = True) -> None:
        """
        SQLクエリを実行してデータを取得
        
        Args:
            strSQL (str): SQLクエリ
            astype_datetime (bool): datetime型に変換するかどうか
        """
        try:
            conn = connect(self.db_path)
            self.df = pd.read_sql_query(strSQL, conn)
            if astype_datetime:
                self.df["datetime"] = pd.to_datetime(self.df["datetime"])
            conn.close()
        except Exception as e:
            logger.error(f"SQLクエリ実行失敗: {str(e)}")
            raise TdNetError(f"SQLクエリ実行失敗: {str(e)}")

    def toHTML(self, filename: str = "tdnet_list.html", encoding: str = "utf_8_sig") -> None:
        """
        データをHTMLファイルに出力
        
        Args:
            filename (str): 出力ファイル名
            encoding (str): エンコーディング
        """
        try:
            self.df.to_html(filename, render_links=True, encoding=encoding)
        except Exception as e:
            logger.error(f"HTML出力失敗: {str(e)}")
            raise TdNetError(f"HTML出力失敗: {str(e)}")

    def toCSV(self, filename: str = "tdnet_list.csv", encoding: str = "utf_8_sig") -> None:
        """
        データをCSVファイルに出力
        
        Args:
            filename (str): 出力ファイル名
            encoding (str): エンコーディング
        """
        try:
            if self.df is not None and not self.df.empty:
                if os.path.exists(filename):
                    existing_df = pd.read_csv(filename, encoding=encoding)
                    combined_df = pd.concat([existing_df, self.df]).drop_duplicates(
                        subset=["pdf"]
                    )
                    combined_df.to_csv(filename, index=False, encoding=encoding)
                else:
                    self.df.to_csv(filename, index=False, encoding=encoding)
        except Exception as e:
            logger.error(f"CSV出力失敗: {str(e)}")
            raise TdNetError(f"CSV出力失敗: {str(e)}")

def xbrl_to_csv(xbrl_paths: Dict[str, str]) -> pd.DataFrame:
    """
    XBRLファイルからデータを抽出してCSV形式に変換
    
    Args:
        xbrl_paths (Dict[str, str]): XBRLファイルのパス辞書
        
    Returns:
        pd.DataFrame: 変換されたデータ
    """
    try:
        all_xbrl_data = []
        
        for file_type, xbrl_path in xbrl_paths.items():
            with open(xbrl_path, "r", encoding="utf-8") as f:
                xbrl_content = f.read()
            
            soup = BeautifulSoup(xbrl_content, "html.parser")
            elements = soup.find_all(["ix:nonnumeric", "ix:nonfraction"])
            
            for element in elements:
                pname, name = element.get("name", "").split(":")
                data = {
                    "file_type": file_type,
                    "contextRef": element.get("contextref", ""),
                    "name_prefix": pname,
                    "name": name,
                    "format": element.get("format", ""),
                    "unitRef": element.get("unitref", ""),
                    "decimals": element.get("decimals", ""),
                    "scale": element.get("scale", ""),
                    "sign": element.get("sign", ""),
                    "text": element.text.strip(),
                }
                all_xbrl_data.append(data)
        
        df_xbrl = pd.DataFrame(all_xbrl_data)
        df_xbrl["text"] = df_xbrl.apply(
            lambda row: (
                pd.to_numeric(row["text"].replace(",", ""), errors="coerce")
                * (-1 if row["sign"] == "-" else 1)
            )
            if len(row["scale"]) > 0
            else row["text"]
        )
        return df_xbrl
        
    except Exception as e:
        logger.error(f"XBRL変換失敗: {str(e)}")
        raise TdNetError(f"XBRL変換失敗: {str(e)}")