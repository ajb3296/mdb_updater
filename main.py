import os
import shutil
import sqlite3

class Main():
    def __init__(self):
        self.last_mdb = None
        self.new_mdb = None
        self.result_mdb = "result/result.mdb"
        self.target_table = [["character_system_text", [0, 1, 9], ["character_id", "voice_id", "lip_sync_data"], ["int", "int", "str"], 2, "text"],
                             ["race_jikkyo_comment", [0, 1], ["id", "group_id"], ["int", "int"], 2, "message"],
                             ["race_jikkyo_message", [0, 1], ["id", "group_id"], ["int", "int"], 2, "message"],
                             ["text_data", [0, 1, 2], ["id", "category", "`index`"], ["int", "int", "int"], 3, "text"]
        ]
        self.fail_apply = []
        self.fail_search = []

    def copy_result_mdb(self):
        shutil.copy2(self.new_mdb, self.result_mdb)

    def get_path(self, status = None):
        if status is not None: # For test
            last_mdb = "master.mdb"
            new_mdb = "master-update.mdb"
        else:
            last_mdb = input("이전까지 번역해뒀던 mdb 파일의 경로를 입력해 주세요 : ").strip()
            new_mdb = input("새로 업데이트된 mdb 파일의 경로를 입력해 주세요 : ").strip()

        self.last_mdb = last_mdb
        self.new_mdb = new_mdb

    def get_data(self, path, table): # mdb 파일에서 데이터 가져오기
        conn = sqlite3.connect(path)
        c = conn.cursor()

        c.execute(f"SELECT * FROM {table}")
        data = c.fetchall()
        conn.close()

        return data
    
    def find_data(self, path, table, index_data: list):
        conn = sqlite3.connect(path, isolation_level=None)
        c = conn.cursor()
        command = f"SELECT * FROM {table} WHERE " # 기초 명령어 세팅
        where = ""
        for table_from_target_table in self.target_table:
            if table_from_target_table[0] == table: # 테이블 명이 같다면
                for column_title in enumerate(table_from_target_table[2]):
                    if table_from_target_table[3][column_title[0]] == "str": # str 타입이라면
                        where += f"{column_title[1]}='{index_data[column_title[0]]}'"
                    else: # int 타입이라면
                        where += f"{column_title[1]}={index_data[column_title[0]]}"
                    if column_title[0] != len(table_from_target_table[2]) - 1: # 마지막이 아니라면
                        where += " AND "
                break

        try:
            c.execute(command + where)
        except sqlite3.OperationalError:
            conn.close()
            return None, None
        temp = c.fetchone()
        conn.close()
        if temp is not None:
            return temp[table_from_target_table[4]], where
        else:
            return None, None

    def set_message(self, path, table, where, message):
        conn = sqlite3.connect(path, isolation_level=None)
        c = conn.cursor()
        for table_from_target_table in self.target_table:
            if table_from_target_table[0] == table:
                message = message.replace("'", "''") # syntax error 방지
                command = f"UPDATE {table} SET {table_from_target_table[5]}='{message}' WHERE {where}"
                try:
                    c.execute(command)
                except sqlite3.OperationalError as error_log:
                    return error_log
                break
        conn.close()
        return True

if __name__ == "__main__":
    main = Main()

    # 경로 설정
    main.get_path()

    # 업데이트된 파일 복사해서 result.mdb 파일 생성
    main.copy_result_mdb()

    # mdb 파일에서 데이터 가져오기
    data = main.get_data(main.new_mdb, "text_data")

    for table_data in main.target_table: # target_table에 있는 테이블들을 순회
        new_data = main.get_data(main.result_mdb, table_data[0]) # 새로운 mdb 파일에서 데이터 가져오기
        for element in new_data: # mdb 파일에서 가져온 데이터를 순회
            temp = [] # 이전 데이터에서 찾기위한 위치 데이터 리스트
            for index in table_data[1]:
                temp.append(element[index])
            msg, where = main.find_data(main.last_mdb, table_data[0], temp) # 새 데이터 기반 과거 데이터 검색
            if msg is not None and where is not None:
                status = main.set_message(main.result_mdb, table_data[0], where, msg) # 과거 데이터를 새 데이터에 적용
                if status != True: # 적용 실패시
                    main.fail_apply.append([table_data[0], temp, msg, status])
                    print(f"적용 실패 : {table_data[0]} {temp} {msg}\nError log : {status}")
            else: # 검색 실패시
                main.fail_search.append([table_data[0], temp, msg])
                print(f"검색 실패 : {table_data[0]} {temp} {msg}")
    
    # 로그 폴더 생성
    try:
        shutil.rmtree('logs')
    except FileNotFoundError:
        pass
    os.mkdir("logs")
    text = ""
    for i in main.fail_search:
        text += f"Table : {i[0]}, Where : {i[1]}\n"
    file = open("logs/fail_search.txt", "w", encoding = 'UTF-8')
    file.write(text)
    file.close()

    text = ""
    for i in main.fail_apply:
        text += f"Table : {i[0]}, Where : {i[1]}, Message : {i[2]}, Error log : {i[3]}\n"
    file = open("logs/fail_apply.txt", "w", encoding = 'UTF-8')
    file.write(text)
    file.close()