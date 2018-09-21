import csv
import logging
import os

local_path = 'C:\\Users\\admin\\Downloads\\'
old_filename = 'old.csv'
new_filename = 'new.csv'


old_file = os.path.join(local_path, old_filename)
new_file = os.path.join(local_path, new_filename)

logging.basicConfig(filename='csv_file_sorter.log', level=logging.INFO, format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


class CsvSortFile:
    def __init__(self, old, new):
        self.old = old
        self.new = new

    def sorter(self,):
        with open(self.old, 'r', newline='') as csv_file, open(self.new, 'w', newline='') as new_csv_file:
            csv_writer = csv.writer(new_csv_file)
            csv_reader = csv.reader(csv_file)
            old_file_count = 0
            new_file_count = 0
            for line in csv_reader:
                old_file_count += 1
                if 'REPLENISHMENT' in line[1] or 'WITHDRAWAL' in line[1]:
                    csv_writer.writerow([line[0], line[2]])
                    new_file_count += 1
            logging.info("Old file has {old_count} rows".format(old_count=old_file_count))
            logging.info("New file has {new_count} rows".format(new_count=new_file_count))


sorter = CsvSortFile(old_file, new_file)

if __name__ == '__main__':
    sorter.sorter()
