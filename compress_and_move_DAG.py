import fnmatch
import gzip
import logging
import os
import shutil
from multiprocessing import Pool

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

__desc__ = '...'

args = {
    'owner': 'osiniy',
    'start_date': airflow.utils.dates.days_ago(2),
}

directory_data = Variable.get('directory_data')
directory_compress = Variable.get('compress_dir')


def csv_files_compress(file):
    check = os.path.join(directory_data, file)
    if os.path.isfile(check) and not file.endswith('.gz'):
        gz_filename = file + '.gz'
        with open(os.path.join(directory_data, file), 'rb') as src_file, gzip.open(
                os.path.join(directory_data, gz_filename), 'wb') as gz_file:
            shutil.copyfileobj(src_file, gz_file)
        logging.info("[{f}] was compressed in [{f}.gz]".format(f=file))


with DAG(dag_id='file_comparison_dag', default_args=args, schedule_interval='0 9 * * *', catchup=False) as dag:

    def pool_compressor():
        work_dir = os.listdir(directory_data)
        with Pool(10) as pool:
            pool.map_async(csv_files_compress, work_dir)
            pool.close()
            pool.join()

    def transfer_files(**context):
        oper_date = context.get('templates_dict').get('date')
        dst_folder = os.path.join(directory_compress, oper_date)
        os.makedirs(dst_folder, exist_ok=True)
        logging.info('[{}] was created.'.format(dst_folder))

        file_list = [os.path.join(directory_data, f) for f in os.listdir(
            directory_data) if fnmatch.fnmatch(f, '*.gz')]

        for file in file_list:
            shutil.move(file, os.path.join(dst_folder, os.path.basename(file)))
            logging.info(
                "[{f}] was moved to [{d}].".format(f=file, d=os.path.join(dst_folder, os.path.basename(file))))

    def cleaner():
        work_dir = os.listdir(directory_data)
        for filename in work_dir:
            file = os.path.join(directory_data, filename)
            try:
                os.remove(file)
            except IsADirectoryError:
                shutil.rmtree(file)
                logging.info("[{}] directory was removed.".format(file))
            logging.info("[{}] file was removed.".format(file))

    start = DummyOperator(task_id='start')

    csv_compress = PythonOperator(
        task_id='csv_compress',
        provide_context=False,
        python_callable=pool_compressor
    )

    files_transfer = PythonOperator(
        task_id='files_transfer',
        python_callable=transfer_files,
        provide_context=True,
        templates_dict={'date': '{{ ds }}'}
    )

    dir_cleaner = PythonOperator(
        task_id='dir_cleaner',
        provide_context=False,
        python_callable=cleaner
    )

    finish = DummyOperator(task_id='finish')

    start >> csv_compress >> files_transfer >> dir_cleaner >> finish
