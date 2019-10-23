import logging
import os
from zipfile import ZipFile
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email

# channel_list = Variable.get('mail_channel_sender', deserialize_json=True)
channel_list = {
    "mail": [
        "some_user@gmail.com",
        "new_user@gmail.com"
    ],
    "channels": {
        "success": {
            "subject_body": "Tasks with success status {}.",
            "files": {
                "{{ ds }}-success.csv.tar.gz",
                "{{ yesterday_ds }}-success.csv.tar.gz",
                "{{ tomorrow_ds }}-success.csv.tar.gz"
            },
            "arch_name": "success_channel_{}.zip"
        },
        "failed": {
            "subject_body": "Tasks with fail status {}.",
            "files": {
                "{{ ds }}-failed.csv.tar.gz",
                "{{ yesterday_ds }}-failed.csv.tar.gz",
                "{{ tomorrow_ds }}-failed.csv.tar.gz"
            },
            "arch_name": "fail_channel_{}.zip"
        },
        "delayed": {
            "subject_body": "Tasks with delay status {}.",
            "files": {
                "{{ ds }}-delayed.csv.tar.gz",
                "{{ yesterday_ds }}-delayed.csv.tar.gz",
                "{{ tomorrow_ds }}-delayed.csv.tar.gz"
            },
            "arch_name": "delay_channel_{}.zip"
        },
    }
}
data_folder = Variable.get('directory_data')

args = {
    'owner': 'osiniy',
    'start_date': airflow.utils.dates.days_ago(2),
}


def channel_sender(**context):
    channel = context['templates_dict']['channel']
    date = context['templates_dict']['date']

    logging.info("Operation date is: {}.".format(date))
    logging.info("Work directory is: {}.".format(data_folder))

    files = [
        os.path.join(data_folder, channel_list.get('channels').get(channel).get('files').get('error').format(
            date)),
        os.path.join(data_folder, channel_list.get('channels').get(channel).get('files').get('running').format(
            date)),
        os.path.join(data_folder, channel_list.get('channels').get(channel).get('files').get('state').format(
            date)),
        os.path.join(data_folder, channel_list.get('channels').get(channel).get('files').get('success').format(
            date)),
        os.path.join(data_folder, channel_list.get('channels').get(channel).get('files').get('retry').format(
            date)),
        os.path.join(data_folder, channel_list.get('channels').get(channel).get('files').get('delay').format(
            date)),
        os.path.join(data_folder, channel_list.get('channels').get(channel).get('files').get('failed').format(
            date)),
        os.path.join(data_folder, channel_list.get('channels').get(channel).get('files').get('queued').format(
            date)),
        os.path.join(data_folder, channel_list.get('channels').get(channel).get('files').get('started').format(
            date)),
        os.path.join(data_folder, channel_list.get('channels').get(channel).get('files').get('finished').format(
            date))
    ]

    file_list = [file for file in files if os.path.isfile(file) and os.path.basename(file) != 'dummy']

    if file_list:

        attachments_list = []
        oversize_files = []

        arch_size_limit = 25165824
        current_size = 0

        sorted_files = sorted(file_list, key=os.path.getsize)

        for f in sorted_files:
            current_size += os.path.getsize(f)
            if current_size <= arch_size_limit:
                attachments_list.append(f)
            else:
                oversize_files.append(f)

        content = """"""
        for f in attachments_list:
            content += '<p>File {} was sent in archive.</p>'.format(f.split('/')[-1])
        for msg in oversize_files:
            content += '<p>Oversize file: {f}, size is: {s} bytes.</p>'.format(f=msg.split('/')[-1],
                                                                               s=os.path.getsize(msg))

        arch_name = channel_list.get('channels').get(channel).get('arch_name')
        arch_name = os.path.join(data_folder, arch_name.format(channel))

        with ZipFile(arch_name, 'w') as zip_object:
            for file in attachments_list:
                zip_object.write(file, os.path.basename(file))

        send_email(to=channel_list['mail'],
                   subject=channel_list.get('channels').get(channel).get('subject_body').format(date),
                   html_content=content, files=[arch_name], mime_subtype='mixed', mime_charset='utf-8')
        for f in attachments_list:
            logging.info("File [{}] was sent.".format(f))
        if oversize_files:
            for f in oversize_files:
                logging.info("File [{}] is oversize gmail restrictions.".format(f))
        os.remove(arch_name)


with DAG(dag_id='file_comparison_dag', default_args=args, schedule_interval='0 9 * * *', catchup=False) as dag:
    dummy = DummyOperator(
        task_id='dummy'
    )

    for channel in ['spark', 'pandas']:
        mail_send_channel_sender = PythonOperator(
            task_id='mail_send_files_{}'.format(channel),
            python_callable=channel_sender,
            templates_dict={
                'channel': '{}'.format(channel),
                'date': '{{ ds }}'
            },
            provide_context=True
        )

        dummy >> mail_send_channel_sender
