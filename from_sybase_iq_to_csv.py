import os
import pyodbc
import zipfile
import csv
import sys
from contextlib import closing

driver = '{SQL Server}'
server = '192.168.1.1'
user = 'user'
password = 'password'
delimiter = '|'
local_path = 'C:\\Users\\guest\\folder\\'
filename = 'test_file.csv'
local_filepath = os.path.join(local_path,filename)
sql_query = ''' select * from airflow_table'''


def from_iq_to_csv(driver, server, user, password, delimiter, local_filepath):
    try:
        with closing(pyodbc.connect(pyodbc.connect(driver, server, user, password))) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            cnt = 0
            with open(local_filepath, "w") as f:
                csv_writer = csv.writer(f, delimiter='|')
                csv_writer.writerow([field[0] for field in cursor.description])
                for i, chunk in enumerate(chunks(cursor)):
                    cnt += len(chunk)
                    f.write('\n'.join([delimiter.join([str(r) for r in row]) for row in chunk]))
                    f.write('\n')
                    log.info('{0} pack unloaded. [{1}] total rows'.format(i, cnt))
                f.flush()
                cursor.close()
                conn.commit()
                conn.close()
                log.info("Dumping finished")
                arch = zipfile.ZipFile(os.path.splitext(local_filepath)[0] + '.zip', "w", zipfile.ZIP_DEFLATED)
                arch.write(local_filepath, os.path.basename(local_filepath))
                os.remove(local_filepath)

    except Exception:
        print(" Error while writing from database to csv")
        print(sys.exc_info()[1])


def chunks(cur):
    while True:
        rows = cur.fetchmany()
        if not rows:
            break
        yield rows


if __name__ == "__main__":
    from_iq_to_csv(driver, server, user, password, delimiter, local_filepath)