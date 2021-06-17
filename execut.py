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

  with open(file_name, 'r') as f:
    reader = csv.reader(f)
    for row in reader:
      c += 1
      cursor.execute('''update SAMPLE_TABLE set COL1 = :arg1 where COL2 = :arg2''', arg1=row[0], arg2=row[2]) #updateの実行

      #1000件更新ごとにコミット
      if c % batch_size == 0:
        print("commit:{0}".format(c), flush=True)
        connect.commit() #コミット
        cursor.close() #コミット確定
        cursor = connect.cursor() #カーソルの再オープン

  connect.commit() #最後にコミット
  cursor.close()

print("script end.")
