
import requests
import pandas as pd
import time
import logging
from argparse import ArgumentParser


def get_range():
    parser = ArgumentParser()
    parser.add_argument('-r', '--range', type=int, dest='range',
                        help="range", metavar="Range")

    args = parser.parse_args()

    if (args.range):
        return(args.range)

    return 1


def save_to_csv(animes):
    try:
        df = pd.json_normalize(animes)
        df.to_csv('../../dataset/anime.csv', mode='a',
                  index=False, header=False)
    except:
        logging.error(time.strftime('<%Y-%m-%dT%H:%M:%SZ> ',
                                    time.gmtime()) + 'Error while saving anime data')


logging.basicConfig(filename='../../mal_anime_parser.log', level=logging.INFO)

base_url = 'https://api.jikan.moe/v3'
up_to = get_range()
related_types = ['Adaptation', 'Alternative version', 'Alternative setting',
                 'Sequel', 'Prequel', 'Side story', 'Spin-off', 'Character', 'Summary', 'Other']

# Get the starting point
master = '../../dataset/anime.csv'
start = 1
try:
    mDf = pd.read_csv(master)
    start = int(mDf.iloc[-1, 0]) + 1
    print('Start is: ' + str(start))
except pd.io.common.EmptyDataError:
    print('Anime dataset is empty!')
    start = 1
except IndexError:
    print('Anime dataset is empty!')
    start = 1

animes = []

count = 0

for i in range(start, start + up_to):
    apiUrl = f'{base_url}/anime/{i}'
    response = requests.get(apiUrl)

    if response.status_code == 200:
        jsonResult = response.json()

        anime = {}
        related_animes = []

        for type in related_types:
            try:
                for related in jsonResult['related'][type]:
                    related_anime = {}
                    related_anime['id'] = str(jsonResult['mal_id'])
                    related_anime['related_anime_id'] = str(related['mal_id'])
                    related_anime['name'] = related['name']
                    related_anime['type'] = related['type']
                    related_animes.append(related_anime)
            except KeyError:
                pass

        anime['id'] = jsonResult['mal_id']
        anime['title'] = jsonResult['title']
        anime['title_english'] = jsonResult['title_english']
        anime['title_japanese'] = jsonResult['title_japanese']
        anime['title_synonyms'] = jsonResult['title_synonyms']
        anime['type'] = jsonResult['type']
        anime['source'] = jsonResult['source']
        anime['episodes'] = jsonResult['episodes']
        anime['status'] = jsonResult['status']
        anime['airing'] = jsonResult['airing']
        anime['aired_from'] = jsonResult['aired']['from']
        anime['aired_to'] = jsonResult['aired']['to']
        anime['aired'] = jsonResult['aired']['string']
        anime['duration'] = jsonResult['duration']
        anime['rating'] = jsonResult['rating']
        anime['score'] = jsonResult['score']
        anime['scored_by'] = jsonResult['scored_by']
        anime['rank'] = jsonResult['rank']
        anime['popularity'] = jsonResult['popularity']
        anime['members'] = jsonResult['members']
        anime['favorites'] = jsonResult['favorites']
        anime['synopsis'] = jsonResult['synopsis']
        anime['background'] = jsonResult['background']
        anime['premiered'] = jsonResult['premiered']
        anime['broadcast'] = jsonResult['broadcast']
        anime['producers'] = [x['name'] for x in jsonResult['producers']]
        anime['licensors'] = [x['name'] for x in jsonResult['licensors']]
        anime['studios'] = [x['name'] for x in jsonResult['studios']]
        anime['genres'] = [x['name'] for x in jsonResult['genres']]
        anime['related_anime'] = related_animes
        anime['opening_themes'] = jsonResult['opening_themes']
        anime['ending_themes'] = jsonResult['ending_themes']
        anime['timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

        animes.append(anime)
        count = count+1

        logging.info(time.strftime('<%Y-%m-%dT%H:%M:%SZ> ', time.gmtime()) + 'Successfully retrieved: ' +
                     jsonResult['title'] + ' id=' + str(i))

    elif response.status_code == 404:
        logging.info(time.strftime('<%Y-%m-%dT%H:%M:%SZ> ',
                                   time.gmtime()) + 'Anime id=' + str(i) + ' does not exist')
    else:
        logging.info(time.strftime('<%Y-%m-%dT%H:%M:%SZ> ', time.gmtime()) + 'Error while retrieving ' + str(i) +
                     ' with status code ' + str(response.status_code))
    time.sleep(5)

    if(count % 10 == 0 and count != 0):
        save_to_csv(animes)
        animes = []
        related_animes = []

save_to_csv(animes)
