import random

file_path = "C:\\Users\\admin\\Downloads\\films.txt"
new_file = "C:\\Users\\admin\\Downloads\\download.txt"


class RandomChoiceFilm:
    """ с помощью скрипта открывается список с фильмами, случайным образом выбирается колличество фильмов заданое
    пользователем, параллельно открывается второй файл в который записываются фильмы для загрузки. Итоговый этап, это
     перезапись первоначального файла уже без фильмов, которые указаны для загрузки """

    def __init__(self, film_list, download_list):
        self.film_list = film_list
        self.download_list = download_list

    def film_shaker(self, ):
        with open(self.film_list, 'r') as my_films, open(self.download_list, 'w') as new:
            reader = my_films.read().split('\n')
            print('List contains ' + str(len(reader)) + ' films.')
            quantity = int(input("How many films will you watch? "))
            choice = random.sample(reader, quantity)
            result = '\n'.join(choice)
            print(result)
            # создаем новый список без филмьов, которые указаны для загрузки
            update_list = [x for x in reader if x not in choice]
            new_film_list = '\n'.join(update_list)
            new.writelines(result)
            return new_film_list

    def show(self, new_list):
        with open(self.film_list, 'w') as my_films:
            my_films.writelines(new_list)

    def counter(self,):
        with open(self.film_list, 'r') as my_films:
            film_list = my_films.read().split('\n')
            print('New list contains ' + str(len(film_list)) + ' films.')


choice_movie = RandomChoiceFilm(film_list=file_path, download_list=new_file)


if __name__ == '__main__':
    choice_movie.show(new_list=choice_movie.film_shaker())
    choice_movie.counter()
