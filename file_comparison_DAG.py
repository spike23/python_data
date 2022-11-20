import logging
import os
import shutil
import tarfile

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col, when


args = {
    'owner': 'osiniy',
    'start_date': airflow.utils.dates.days_ago(2),
}


# file_comparison_dag_var = Variable.get('file_comparison_dag_var', deserialize_json=True)
file_comparison_dag_var = {
        "compress_data": "filepath",
        "compare_folder": "filepath",
        "file_list": [
             "{{ ds }}-all.csv.tar.gz",
             "{{ yesterday_ds }}-all.csv.tar.gz",
             "{{ tomorrow_ds }}-all.csv.tar.gz"
         ]
     }
file_list = file_comparison_dag_var['file_list']
directory_data = file_comparison_dag_var['compress_data']
comparison_data = os.path.join(directory_data, 'comparison_data')
comparison_diff = os.path.join(directory_data, 'comparison_diff')
compare_folder = file_comparison_dag_var['compare_folder']


def untar_file(src_file, dest_path):
    """
    unpacked file with tar.gz extension

    :param src_file: compressed file
    :param dest_path: decompressed file
    :return: None
    """
    with tarfile.open(src_file, "r:gz") as tar:
        def is_within_directory(directory, target):
            
            abs_directory = os.path.abspath(directory)
            abs_target = os.path.abspath(target)
        
            prefix = os.path.commonprefix([abs_directory, abs_target])
            
            return prefix == abs_directory
        
        def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
        
            for member in tar.getmembers():
                member_path = os.path.join(path, member.name)
                if not is_within_directory(path, member_path):
                    raise Exception("Attempted Path Traversal in Tar File")
        
            tar.extractall(path, members, numeric_owner=numeric_owner) 
            
        
        safe_extract(tar, path=dest_path)


def strip_extension(file_name, ext):
    """
    strip file extension

    :param file_name: name of file
    :param ext: extension which must be stripped
    :return: file name without extension of file
    """
    return file_name[:-1 * len(ext)] if file_name.endswith(ext) else file_name


with DAG(dag_id='file_comparison_dag', default_args=args, schedule_interval='0 9 * * *', catchup=False) as dag:
    dummy = DummyOperator(
        task_id='dummy'
    )


    def unpacking_files(**context):
        """
        unpack tar.gz files or move other files to destination folder

        :param context: task instance context
        :return: None
        """
        global comparison_data, directory_data, compare_folder
        files = context['templates_dict']['file_list']
        os.makedirs(comparison_data, exist_ok=True)
        file_list_ = [os.path.join(directory_data, i) for i in files if os.path.exists(os.path.join(directory_data, i))]

        for file_ in file_list_:
            if file_.endswith('.tar.gz'):
                untar_file(src_file=file_, dest_path=comparison_data)
            else:
                shutil.move(os.path.join(directory_data, file_), os.path.join(comparison_data, file_))
        logging.info("untar file into {}".format(os.path.join(comparison_data)))
        logging.info(os.listdir(comparison_data))


    def file_compare():
        """
        compare two files and save reconciliation to result file

        :return: None
        """

        files = os.listdir(comparison_data)
        file_list_ = [os.path.join(comparison_data, i) for i in files]

        comparison_files = os.listdir(compare_folder)
        compare_list = [os.path.join(compare_folder, i) for i in comparison_files]
        sorted(file_list_), sorted(compare_list)

        os.makedirs(comparison_diff, exist_ok=True)

        spark = SparkSession.builder.appName(dag.dag_id).config("spark.executor.cores", 8). \
            config("spark.executor.memory", "2g"). \
            config("spark.driver.memory", "2g"). \
            config("spark.driver.maxResultSize", "4g").getOrCreate()

        for unpacked_file, etalon_file in zip(file_list_, compare_list):

            file_name = unpacked_file.split('/')[-1]

            if os.stat(unpacked_file).st_size == 0 and os.stat(etalon_file).st_size == 0:
                unpacked_empty_name = os.path.join(comparison_diff,
                                                   'unpacked_etalon_{filename}'.format(filename=file_name))
                shutil.copy(unpacked_file, unpacked_empty_name)
                logging.info('Both files [{}] are empty.'.format(unpacked_file))
                continue
            elif os.stat(unpacked_file).st_size == 0:
                unpacked_empty_name = os.path.join(comparison_diff, 'unpacked_{filename}'.format(filename=file_name))
                shutil.copy2(unpacked_file, unpacked_empty_name)
                logging.info('Unpacked file [{}] is empty.'.format(unpacked_file))
                continue
            elif os.stat(etalon_file).st_size == 0:
                etalon_empty_name = os.path.join(comparison_diff, 'original_{filename}'.format(filename=file_name))
                shutil.copy2(etalon_file, etalon_empty_name)
                logging.info('Original file [{}] is empty.'.format(etalon_file))
                continue

            df1 = spark.read.csv(unpacked_file, header=False, sep=chr(11))
            df2 = spark.read.csv(etalon_file, header=False, sep=chr(11))

            df3 = df1.join(df2, on=[(df1._c0 == df2._c0)], how='outer').filter(
                df1._c0.isNull() | df2._c0.isNull()).select(df1._c0.alias('unpacked'), df2._c0.alias('original'))

            new_column = when(col('unpacked').isNull(), 'original').otherwise('unpacked')
            df4 = df3.withColumn('side', new_column).withColumn('data', coalesce(df3.unpacked, df3.etalon)).select(
                ['side', 'data'])

            temp_dir = os.path.join(comparison_diff, strip_extension(os.path.basename(unpacked_file), '.csv'))
            if df4.count() != 0:
                os.makedirs(temp_dir, exist_ok=True)
                df4.coalesce(1).write.csv(temp_dir, header=False, quote=' ', mode='overwrite')

                mv_file = [i for i in os.listdir(temp_dir) if i.startswith('part-')]

                src = os.path.join(temp_dir, *mv_file)
                dst = os.path.join(comparison_diff, 'diff_{filename}'.format(filename=file_name))
                shutil.move(src, dst)
                shutil.rmtree(temp_dir)
                logging.info('------FINISH-----[{0}][{1}] COMPARING ---------------'.format(unpacked_file, etalon_file))
            logging.info('Files [{0}][{1}] are identical.'.format(unpacked_file, etalon_file))


    untar_files = PythonOperator(
        task_id='untar_files',
        python_callable=unpacking_files,
        provide_context=True,
        templates_dict={'file_list': file_list}
    )

    compare_files = PythonOperator(
        task_id='compare_files',
        python_callable=file_compare,
    )

    dummy >> untar_files >> compare_files
