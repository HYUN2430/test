import MySQLdb
from datetime import datetime
import os
from save_folder import send_to_db
from save_folder import save_folder_path2

conn = MySQLdb.connect(host='localhost', user='username', passwd='password', db='test_db', charset='utf8')
# 違反者db
save_folder_path = R'C:\Users\Username\Desktop\project'


#違法者をDBに入れる
def sql_low_db(save_folder_path):
    # データベースとの接続
    cursor = conn.cursor()
    for filename in os.listdir(save_folder_path): # このフォルダのすべてのファイルを読み込む 
        if filename.endswith('.txt'):
            file_path = os.path.join(save_folder_path, filename)
            print(f"File: {filename}")
            with open(file_path, 'r',encoding='utf-8') as fh:
                #新しいファイルを読み込むたびに以下の変数の初期化が必要
                
                index=0
                for line in fh:
                    line = line.strip()
                    
                    if line.startswith('ID:'):
                        
                            index+=1
                            id_value = line.split(':')[1].strip()
                            print("id", id_value)
                        
                    elif line.startswith('Parking_time:'):
                       
                            index+=1
                            parking_time_value = int(line.split(':')[1].split('日')[0].strip())
                            print("parking", parking_time_value)
                       
                    elif line.startswith('start:'):
                       
                            index+=1
                            start_value = datetime.strptime(line.split(':')[1].strip(), '%Y年%m月%d日%H時%M分')
                            print("start:", start_value)
                       
                    elif line.startswith('end:'):
                       
                            index+=1
                            end_str = line.split(':')[1].strip()
                            # ここでend時間が0000年00月00日00時00分の場合に対応
                            if end_str == '0000年00月00日00時00分':
                                end_value = None  # NULL として扱う
                            else:
                                try:
                                    end_value = datetime.strptime(end_str, '%Y年%m月%d日%H時%M分')
                                except ValueError:
                                    end_value = None  # エラー処理
                                    
                            print("end:", end_value)
                       
                    elif line.startswith('expiration_date:'):
                       
                            index+=1
                            expiration_date_value = datetime.strptime(line.split(':')[1].strip(), '%Y年%m月%d日').date()
                            print("expira:", expiration_date_value)
                       
                    elif line.startswith('priority:'):
                        
                            index+=1
                            priority_value = int(line.split(':')[1].strip())
                            print("priority:", priority_value)
                       
                if index == 6:        
                    #4つのデータが正しく読み取れた場合のみデータベースへの書き込みを行う
                    #end_valueの値を検査しないのは’0000年00月00日00時00分’の場合はnullとして扱うため
                    if id_value is not None and parking_time_value is not None and start_value is not None and  expiration_date_value is not None and priority_value is not None:
                        # MySQLにデータを挿入または更新
                        select_query = 'SELECT * FROM lowbreaker WHERE id = %s;'
                        cursor.execute(select_query, (id_value,))
                        existing_data = cursor.fetchone()


                        if existing_data:
                            # データが既に存在する場合は更新
                            #修正3:修正した方がいいかも(でもチェックするのも負担かかる)?同じの場合は更新する必要がないけど
                            update_query = '''
                                UPDATE lowbreaker
                                SET Parking_time = %s, start = %s, end = %s, expiration_date = %s, priority = %s
                                WHERE id = %s;
                            '''
                            cursor.execute(update_query, (parking_time_value, start_value, end_value, expiration_date_value, priority_value, id_value))
                            
                        else:
                            # データが存在しない場合は挿入
                            insert_query = '''
                                INSERT INTO lowbreaker(id, parking_time, start, end, expiration_date, priority)
                                VALUES (%s, %s, %s, %s, %s, %s);
                            '''
                            cursor.execute(insert_query, (id_value, parking_time_value, start_value, end_value, expiration_date_value, priority_value))
                            
                            #修正4:データが足りない場合はそのファイルネーム表示
                            #例えば、主キーを入力しない場合、エラーになって、プログラミングが止まってしまう。
                            #この場合は、exceptとtryを使って、適当な場所を入れる。
                else:
                    print(f"Missing or invalid in file: {filename}")
                    send_to_db(file_path,filename,save_folder_path2)
                    
                    
    # 変更をコミットし、接続を閉じる
    #commit:トランザクション処理が成功し、完了すること
    conn.commit()
    conn.close()
#実行
sql_low_db(save_folder_path)