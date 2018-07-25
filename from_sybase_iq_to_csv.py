import os
import zipfile
import csv
import pymssql
import sys
from contextlib import closing


server = '192.168.1.1'
user = 'user'
password = 'password'
delimiter = '|'
local_path = 'C:\\Users\\guest\\folder\\'
filename = 'test_file.csv'
local_filepath = os.path.join(local_path,filename)
sql_query = ''' select * from airflow_table'''


class IqToCsv:
    """партиционно записывает данные с сервера в текстовый файл"""
    def __init__(self, srv, usr, passw, delimiter, filepath):
        self.srv = srv
        self.usr = usr
        self.passw = passw
        self.delimiter = delimiter
        self.filepath = filepath

    def from_iq_to_csv(self,):
        try:
            with closing(pymssql.connect(self.srv, self.usr, self.passw, tds_version='4.2',
                                         conn_properties='', charset='cp866')) as conn:
                cursor = conn.cursor()
                cursor.execute(sql_query)
                cnt = 0
                with open(self.filepath, "w", newline='') as f:
                    csv_writer = csv.writer(f, delimiter='|')
                    csv_writer.writerow([field[0] for field in cursor.description])
                    for i, chunk in enumerate(self.chunks(cursor)):
                        cnt += len(chunk)
                        f.write('\n'.join([self.delimiter.join([str(r) for r in row]) for row in chunk]))
                        f.write('\n')
                        print('{qnt} pack unloaded. {count} total rows'.format(qnt=i, count=cnt))
                    f.flush()
                    cursor.close()
                    conn.commit()
                    conn.close()
                    print("Dumping finished")
                arch = zipfile.ZipFile(os.path.splitext(self.filepath)[0] + '.zip', "w", zipfile.ZIP_DEFLATED)
                arch.write(self.filepath, os.path.basename(self.filepath))
                os.remove(self.filepath)

        except Exception:
            print(" Error while writing from database to csv")
            print(sys.exc_info()[1])

    @staticmethod
    def chunks(cur):
        while True:
            rows = cur.fetchmany()
            if not rows:
                break
            yield rows


writer = IqToCsv(srv=server, usr=user, passw=password, delimiter=delimiter, filepath=local_filepath)


if __name__ == "__main__":
    writer.from_iq_to_csv()
