import paramiko
import os
import sys

local_path = 'C:\\Users\\admin\\Downloads\\'
remote_path = '/home/data/airflow/test/'
filename = 'test_file.txt'
local_filepath = os.path.join(local_path, filename)
remote_filepath = os.path.join(remote_path, filename)
host_sftp = "10.1.186.158"
port = 22
password_sftp = "pass"
username_sftp = "usr"


class SFTPTransfer:
    """отправляет файл с одного сервера на другой SFTP сервер"""
    def __init__(self, host, port, usr, passw, from_path, to_path):
        self.host = host
        self.port = port
        self.usr = usr
        self.passw = passw
        self.from_path = from_path
        self.to_path = to_path

    def send_to_sftp(self,):
        # функция для трансфера файла с одного сервера на другой
        try:
            transport = paramiko.Transport((self.host, self.port))
            transport.connect(username=self.usr, password=self.passw)
            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.put(self.from_path, self.to_path)
            sftp.close()
            transport.close()
            print('Upload from {0} to {1} was done.'.format(self.from_path, self.to_path))
        except Exception:
            print('Transfer from {0} to {1} was interrupt'.format(self.from_path, self.to_path))
            print(sys.exc_info()[1])


transfer = SFTPTransfer(host=host_sftp, port=port, usr=username_sftp, passw=password_sftp, from_path=local_filepath,
                        to_path=remote_filepath)


if __name__ == "__main__":
    transfer.send_to_sftp()
