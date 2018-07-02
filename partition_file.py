import os

splitlen = 10
list_of_file = [1, 2, 3]
file_path = 'C:\\Users\\admin\Downloads\\'


def partition_file(splitlen, list_of_file):
    # функция каждый файл по отдельности разбивает на новый файл на splitlen количество строк, после вычитки одного
    # файла переходит к другому и открывает новый файл для записи
    try:
        quontity = 0
        for file in list_of_file:
            with open(file_path + '{0}.txt'.format(file), 'r') as input:
                output_name = 'copy_file_{0}_'.format(file)
                count = 0
                at = 0
                dest = None
                for line in input:
                    if count % splitlen == 0:
                        if dest:
                            dest.close()
                        dest = open(os.path.join(file_path, output_name + str(at) + '.txt'), 'w')
                        at += 1
                        quontity += 1
                    dest.write(line)
                    count += 1
                print(str(count) + " strings in file were writing")
        print(str(quontity) + " files were writing")

    except BaseException:
        print("Error while partitioning")
        raise


def partition_file_version_2(splitlen, list_of_file):
    # функция каждый файл разбивает на новый файл на splitlen количество строк, после вычитки одного файла переходит к
    # другому и не открывая новый файл для записи
    try:
        outputname = 'copy_file_0'
        dest = None
        count = 0
        quontity = 0
        for file in list_of_file:
            with open(file_path + '{0}.txt'.format(file), 'r') as input:
                for n, line in enumerate(input):
                    if count % splitlen == 0:
                        if dest:
                            dest.close()
                        dest = open(os.path.join(file_path,outputname + str(count // splitlen) + '.txt'), 'w')
                        quontity += 1
                        print(outputname + str(count // splitlen) + '.txt')
                    dest.write(line)
                    count += 1
        print(str(quontity) + " files were writing")
        print(str(count) + " strings in file were writing")
        if dest:
            dest.close()
    except BaseException:
        print("Error while partitioning")
        raise


if __name__ == "__main__":
    partition_file(splitlen, list_of_file)

