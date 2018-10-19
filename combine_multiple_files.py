import logging
import os

files_dir = "D:\\test_mail_folder\\"
new_file = "D:\\test_mail_folder\\new.csv"

logging.basicConfig(filename='combine_multiple_files.log', level=logging.INFO, format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


class CombineMultipleFiles:
    """запись нового файла который содержит данные нескольких файлов"""
    def __init__(self, folder, combined_file):
        self.folder = folder
        self.combined_file = combined_file

    def combiner(self):
        folder = os.listdir(self.folder)
        filter_of_files = filter(lambda x: x.endswith('.csv'), folder)
        list_of_file = [file for file in filter_of_files]
        with open(self.combined_file, 'w', newline='\n') as outfile:
            for name in list_of_file:
                with open(os.path.join(files_dir, name)) as infile:
                    outfile.write('\n')
                    outfile.write(infile.read())
        logging.info("Files were combined successfully.")


combiner = CombineMultipleFiles(files_dir, new_file)

if __name__ == '__main__':
    combiner.combiner()
