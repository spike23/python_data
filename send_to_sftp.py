import paramiko
import os

local_path = 'C:\\Users\\guest\\folder\\'
remote_path = 'C:\\Users\\admin\\Downloads\\'
filename = 'test_file.csv'
local_filepath = os.path.join(local_path,filename)
remote_filepath = os.path.join(remote_path,filename)
host_sftp = "10.1.192.100"
port = 22
password_sftp = "pass"
username_sftp = "user"


def send_to_sftp(host_sftp, port, username_sftp, password_sftp, local_filepath, remote_filepath):
    # функция для трансфера файла с одного сервера на другой
    try:
        transport = paramiko.Transport((host_sftp, port))
        transport.connect(username=username_sftp, password=password_sftp)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.put(local_filepath, remote_filepath)
        sftp.close()
        transport.close()
        print ('Upload from {0} to {1}was done.'.format(local_filepath, remote_filepath))
    except BaseException:
        print('Transfer from {0} to {1}was interrupt'.format(local_filepath, remote_filepath))
        raise


if __name__ == "__main__":
    send_to_sftp(host_sftp, port, username_sftp, password_sftp, local_filepath, remote_filepath)
