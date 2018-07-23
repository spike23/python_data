import smtplib
import os
import sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


server = 'smtp.gmail.com'
port = 587
user = 'login.login@gmail.com'
password = 'pass'
filename = 'test_file.csv'
local_filepath = "C:\\Users\\admin\\Downloads\\"
filepath = os.path.join(local_filepath,filename)


class CsvToEmailSender:
    def __init__(self, serv, port, usr, passw, filepath):
        self.serv = serv
        self.port = port
        self.usr = usr
        self.passw = passw
        self.filepath = filepath

    def csv_reader(self,):
        try:
            # открываю вычитываемый файл и параллельно записываю результат в html файл в директорию
            with open(self.filepath, "r") as csv_file, \
                    open(os.path.splitext(self.filepath)[0] + '.html', 'w') as html_file:
                res = '''<table width="100%" cellspacing="0" border="1">'''
                res += '<thead>' + '</thead><tbody>'
                for row in csv_file:
                    res += "<tr><td>" + "</td><td>"''.join([str(x) for x in row.split('|')]) + "</td></tr>"
                res += "</tbody></table>"
                html_file.writelines(res)
                return res
        except Exception:
            print("Error while creating html file from csv.")
            print(sys.exc_info()[1])

    def send_mail(self, res):
        # отправка письма с прикреплнным содержимым (результат работы функции csv_reader)
        try:
            toaddr = ['<nickolay.osiniy@gmail.com>']
            me = 'From: Python_Developer'
            you = 'To: ' + ', '.join(toaddr)
            # Формируем заголовок письма
            msg = MIMEMultipart('mixed')
            msg['Subject'] = 'Test mail with results in mail body.'
            msg['From'] = me
            msg['To'] = ''.join(toaddr[0])  # отправка адресату
            # Формируем письмо
            part1 = MIMEText(res, 'html')
            part2 = MIMEText('<p>There is must be explanation to mail.</p>', 'html')
            #msg.attach(part1)
            msg.attach(part2)
            msg.attach(part1)
            # Подключение
            with (smtplib.SMTP(self.serv, self.port)) as s:
                s.ehlo()
                s.starttls()
                s.ehlo()
                # Авторизация
                s.login(self.usr, self.passw)
                # Отправка письма
                s.sendmail(me, toaddr, msg.as_string())
                s.quit()
        except Exception:
            print("Error while sending mail.")
            print(sys.exc_info()[1])


sender = CsvToEmailSender(serv=server, port=port, usr=user, passw=password, filepath=filepath)

if __name__ == "__main__":
    sender.send_mail(res=csv_reader())