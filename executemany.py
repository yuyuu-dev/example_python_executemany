#!/user/bin/env python3
#-*- conding: utf-8 -*-
import cx_Oracle
import csv
import sys
import os

print("script start.")

TARGET_HOST = "ホスト名"
PORT = "ポート番号"
SERVICE_NAME = "サービス名"
SCHEME_NAME = "スキーマ名"
USERNAME = "ユーザー名"
PASSWORD = os.environ.get('MY') #環境変数から読み込み

file_name = sys.argv[1] #コマンドライン引数から実行ファイル名を受け取る

tns = cx_Oracle.makedsn(TARGET_HOST, PORT, service_name = SERVICE_NAME)
batch_size = 1000
data = []
c = 0

# DBに接続
with cx_Oracle.connect(USERNAME, PASSWORD, tns) as connect:
  cursor = connect.cursor()
  cursor.setinputsizes(10, 20) # COL1とCOL2の最大桁数

  with open(file_name, 'r') as f:
    reader = csv.reader(f)
    sql = "update SAMPLE_TABLE set COL1 = :arg1 where COL2 = :arg2"
    for row in reader:
      c += 1
      data.append((row[0], row[1])) #CSVの内容をdataリストに追加

      if c % batch_size == 0: #dataの長さが1000で割り切れるようになった場合
        cursor.executemany(sql, data) #updateの実行
        data = [] #dataリストを初期化

        print("commit:{0}".format(c), flush=True)
        connect.commit() #コミット
        cursor.close() #コミット確定
        cursor = connect.cursor() #カーソルの再オープン
        cursor.setinputsizes(10, 20) # COL1とCOL2の最大桁数

    if data: #dataリストが残っている場合（1000で割り切れなかった分）
      cursor.executemany(sql, data) #updateの実行

  connect.commit() #最後にコミット
  cursor.close()

print("script end.")
