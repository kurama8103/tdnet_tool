# TDnet解析 by BeautifulSoup

適時開示情報閲覧サービス(https://www.release.tdnet.info/) から開示データ・PDFを取得するプログラム。  
 tdNetクラスにより動作。get***を実行するとtdNet.dfにtdnet開示データが取り込まれる。  
 仕様上取れるデータは30日以内のデータに限る。
 PDFダウンロードやデータ保存はtdNet.dfのデータを基に実行。

Install 
```bash
pip3 install git+https://github.com/kurama8103/tdnet_tool
```  

例
```python
import tdnet_tool
td = tdnet_tool.tdNet()

# 仕様上、30日以内のデータに限る
td.getData_tdnet_byDay('2021-04-21')
td.df.head()

# "修正"をタイトルに含む開示を検索
td.getData_tdnet_KeywordSearch('修正','2021-04-27','2021-04-27')
td.df.head()
```

関数一覧
* データ取得系
    * getData_tdnet_byDay:  
    特定日のtdnet開示一覧を取得し、pandas dataframeを返す。デフォルトは当日。

    * getData_tdnet_KeywordSearch(keyword, date_start, end_date):  
    キーワードによりtdnet開示を検索し、pandas dataframeを返す。

    * getData_SQL:  
    SQL文により過去のtdnet開示を検索し、pandas dataframeを返す。あらかじめtoSQL関数でデータを保存しておく必要がある。

* PDFダウンロード
    * download_PDF_tdnet:  
    pandas dataframeのデータから適時開示PDFをダウンロードする。

* データ保存系
    * toHTML, toCSV:  
    pandasの機能を利用。

    * toSQL, _insert_DB, _createDB:  
    pandas dataframeをsqlite3にINSERTする。_insert_DBで一時テーブルへのINSERTを挟むことでデータの重複を防ぐ。_createDBは初回データ作成用。