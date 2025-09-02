import requests
from bs4 import BeautifulSoup
import time

def fetch_artist_names_from_kworb():
    soup = BeautifulSoup(requests.get("https://kworb.net/itunes/extended.html").content, 'html.parser')
    artist_names = []
    for row in soup.select('#artistsext tr:has(td)'):
        cols = row.find_all('td')
        name = cols[1].text.strip()
        artist_names.append(name)
    return artist_names

def save_names(names, entity):
    file = f'/Users/dieulinh/FSDS/BI/spotify/data/kworb/{entity}_names.txt'
    with open(file, 'w', encoding='utf-8') as f:
        for name in names:
            f.write(name + '\n')
artist_names = fetch_artist_names_from_kworb()
save_names(artist_names, 'artist')