#csvファイルをtxtファイルに変換
import csv
import os
from datetime import datetime
import MySQLdb
# CSVファイルのパスを設定
csv_dir_path = R'C:\Users\Username\Desktop\project\csvfile'
text_file_path = R'C:\Users\Username\Desktop\project\textfile'
conn = MySQLdb.connect(host='localhost', user='username', passwd='password', db='test_db', charset='utf8')
# CSVから抽出したい列のインデックスを設定(1,2,5,6列を抽出)
target_column_indices = [0, 1, 4, 5]  
# データベースとの接続
cursor = conn.cursor()
for csv_file_name in os.listdir(csv_dir_path):
    if csv_file_name.endswith('.csv'): 
        csv_file_path = os.path.join(csv_dir_path, csv_file_name)
        epc_values = []
        count_values = []
        start_values = []
        end_values = []

        #csvファイルを開く
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file)
            
            # ヘッダーをスキップ（最初の行がヘッダーの場合）
            next(csv_reader)
            
            for index, row in enumerate(csv_reader, start=1):
                
                #countの値が０ではない場合
                if row[1] != '0':
                    # 1 列目、2 列目、5 列目, 6 列目の値をリストに保存
                    #print("row is ", row) #行の値確認用
                    epc_values.append(row[0])
                    count_values.append(row[1])
                    start_values.append(row[5])
                    end_values.append(row[4])
                    # MySQLにデータを挿入または更新
                    select_query = 'SELECT ID FROM ID WHERE EPC = %s;'
                    cursor.execute(select_query, (row[0],))
                    result=cursor.fetchone()
                    # Check if a result is found
                    if result:
                    # Extract the ID value from the result
                        name = result[0]
                        print(f"The ID is: {name}")
                        # 空のファイルを作成（名前はepc.txt）
                        filename = os.path.join(text_file_path, f"{name}.txt")
             
                        # 2 つの日付文字列
                        date_str1 = start_values[index-1]
                        date_str2 = end_values[index-1]

                        # 日付形式を指定
                        date_format = "%m/%d/%Y %I:%M:%S %p"

                        # 文字列を datetime 形式に変換
                        date_object1 = datetime.strptime(date_str1, date_format)
                        date_object2 = datetime.strptime(date_str2, date_format)
                        
                        # 2 つの日付の差を計算（timedelta 形式で返される）
                        time_difference = date_object2 - date_object1
                        
                        # timedelta オブジェクトの属性を使用して分を抽出
                        minutes_difference = time_difference.total_seconds() / 60

                        # 日付を指定された形式の文字列に変換
                        formatted_date1 = date_object1.strftime("%Y年%m月%d日%H時%M分")
                        formatted_date2 = date_object2.strftime("%Y年%m月%d日%H時%M分")


                        # 差が 1 分以上かどうかを確認
                        if minutes_difference >= 1:
                            # メモ帳に保存する文字列を生成
                            memo_string = f"ID:{name}\nParking_time:0\nstart:{formatted_date1}\nend:{formatted_date2}\nexpiration_date:0\npriority:1"

                            # ファイルに書き込み
                            with open(filename, "w", encoding="utf-8") as memo_file:
                                memo_file.write(memo_string)
                                
                                
                        #end時間を0000年00月00日00時00分に保存する   
                        else:
                            # メモ帳に保存する文字列を生成
                            memo_string = f"ID:{name}\nParking_time:0\nstart:{formatted_date1}\nend:0000年00月00日00時00分\nexpiration_date:0\npriority:1"

                            # ファイルに書き込み
                            with open(filename, "w", encoding="utf-8") as memo_file:
                                memo_file.write(memo_string)

                    
                #countの値が０の場合
                else:
                    epc_values.append(None)
                    count_values.append(None)
                    start_values.append(None)
                    end_values.append(None)
# 変更をコミットし、接続を閉じる
#commit:トランザクション処理が成功し、完了すること
conn.commit()
conn.close()