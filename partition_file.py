import os
import sys


file_path = "C:\\Users\\admin\\Downloads\\temp\\"
folder = os.listdir(file_path)
filter_of_files = filter(lambda x: x.endswith('.txt'), folder)
list_of_file = [file for file in filter_of_files]
splitlen = 100


class PartitionFiles:
    """ разбивает файлы на опционально указанные партиции """
    def __init__(self, filepath, splitlength, filelist):
        self.filepath = filepath
        self.splitlength = splitlength
        self.filelist = filelist

    def partition_file(self,):
        """ функция каждый файл по отдельности разбивает на новый файл на splitlen количество строк, после вычитки
            одного файла переходит к другому и открывает новый файл для записи """
        try:
            quontity = 0
            for file in self.filelist:
                with open(file_path + '{0}'.format(file), 'r') as input:
                    output_name = 'copy_file_{0}_'.format(file)
                    count = 0
                    at = 0
                    dest = None
                    for line in input:
                        if count % self.splitlength == 0:
                            if dest:
                                dest.close()
                            dest = open(os.path.join(file_path, output_name + str(at) + '.txt'), 'w')
                            at += 1
                            quontity += 1
                        dest.write(line)
                        count += 1
                    print(str(count) + " strings in file were writing")
            print(str(quontity) + " files were writing")

        except Exception:
            print("Error while partitioning")
            print(sys.exc_info()[1])

    def partition_file_version_2(self,):
        """ функция каждый файл разбивает на новый файл на splitlen количество строк, после вычитки одного файла
            переходит к другому и не открывая новый файл для записи """
        try:
            outputname = 'copy_file_0'
            dest = None
            count = 0
            quontity = 0
            for file in self.filelist:
                with open(file_path + '{0}'.format(file), 'r') as input:
                    for n, line in enumerate(input):
                        if count % self.splitlength == 0:
                            if dest:
                                dest.close()
                            dest = open(os.path.join(file_path,outputname + str(count // self.splitlength) + '.txt'), 'w')
                            quontity += 1
                            print(outputname + str(count // self.splitlength) + '.txt')
                        dest.write(line)
                        count += 1
            print(str(quontity) + " files were writing")
            print(str(count) + " strings in file were writing")
            if dest:
                dest.close()
        except Exception:
            print("Error while partitioning")
            print(sys.exc_info()[1])


partition = PartitionFiles(filepath=file_path, splitlength=splitlen, filelist=list_of_file)

if __name__ == "__main__":
    partition.partition_file()
    #  partition.partition_file_version_2()
