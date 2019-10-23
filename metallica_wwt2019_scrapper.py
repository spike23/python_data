import matplotlib.pyplot as plt
import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup

events = [
    '2019-08-25-mannheim-germany', '2019-08-23-munich-germany', '2019-08-21-warsaw-poland',
    '2019-08-18-prague-czech-republic', '2019-08-16-vienna-austria', '2019-08-14-bucharest-romania',
    '2019-07-21-moscow-russia', '2019-07-18-tartu-estonia', '2019-07-16-hameenlinna-finland',
    '2019-07-13-trondheim-norway', '2019-07-11-copenhagen-denmark', '2019-07-09-gothenburg-sweden',
    '2019-07-06-berlin-germany', '2019-06-20-london-england', '2019-06-18-manchester-england',
    '2019-06-16-brussels-belgium', '2019-06-13-cologne-germany', '2019-06-11-amsterdam-netherlands',
    '2019-06-08-slane-castle', '2019-05-12-paris-france', '2019-05-10-zurich-switzerland',
    '2019-05-08-milan-italy', '2019-05-05-barcelona-spain', '2019-05-03-madrid-spain', '2019-05-01-lisbon-portugal'
]

all_songs = []

for event in events:
    quote_page = 'https://www.metallica.com/events/{event}.html'.format(event=event)
    page = urlopen(quote_page)
    soup = BeautifulSoup(page, 'html.parser')
    name_box = soup.findAll('a', attrs={'class': 'songName'})
    for film in name_box:
        all_songs.append(film.text.strip())

songs = list(set(all_songs))

with open('metallica_uniqe_songs_list.txt', 'w') as f:
    f.writelines("%s\n" % song for song in songs)

width = 10
height = 6
plt.figure(figsize=(width, height))
plt.xlim([1, 25])
pd.Series(all_songs).value_counts().plot('bar', color='g')
plt.title('Metallica songs of WorldWiredTour')
plt.xticks(rotation=80)
plt.tight_layout()
plt.savefig('metallica.png', dpi=450)
