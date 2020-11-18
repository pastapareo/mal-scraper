
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


def get_person(id):
    apiUrl = f'{base_url}/person/{i}'

    person = {}
    response = object()

    try:
        response = requests.get(apiUrl)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        tries = 0
        while tries < 5 and response.status_code != 200:
            tries += 1
            response = requests.get(apiUrl)
    except requests.exceptions.ConnectionError as e:
        logging.error(time.strftime('<%Y-%m-%dT%H:%M:%SZ> ',
                                    time.gmtime()) + 'Connection error: ' + e.strerror)
    except requests.exceptions.HTTPError as e:
        if (e.response.status_code):
            logging.info(time.strftime('<%Y-%m-%dT%H:%M:%SZ> ',
                                       time.gmtime()) + 'Person id=' + str(i) + ' does not exist')
        else:
            logging.info(time.strftime('<%Y-%m-%dT%H:%M:%SZ> ', time.gmtime()) +
                         'Error while retrieving ' + str(i) + ' with status code ' + str(response.status_code))
    else:
        jsonResult = response.json()

        person['id'] = str(jsonResult['mal_id'])
        person['name'] = jsonResult['name']
        person['given_name'] = jsonResult['given_name']
        person['family_name'] = jsonResult['family_name']
        person['alternate_names'] = jsonResult['alternate_names']
        person['birthday'] = jsonResult['birthday']
        person['member_favorites'] = str(jsonResult['member_favorites'])
        person['about'] = jsonResult['about']
        person['voice_acting_roles'] = [{'role': x['role'],
                                         'anime_id': str(x['anime']['mal_id']),
                                         'anime_name': x['anime']['name'],
                                         'character_id': str(x['character']['mal_id']),
                                         'character_name': x['character']['name']}
                                        for x in jsonResult['voice_acting_roles']]
        person['anime_staff_positions'] = [{'position': x['position'],
                                            'anime_id': str(x['anime']['mal_id']),
                                            'anime_name': x['anime']['name']}
                                           for x in jsonResult['anime_staff_positions']]
        person['published_manga'] = [{'position': x['position'],
                                      'manga_id': str(x['manga']['mal_id']),
                                      'manga_name': x['manga']['name']}
                                     for x in jsonResult['published_manga']]
        person['timestamp'] = time.strftime(
            '%Y-%m-%dT%H:%M:%SZ', time.gmtime())

        logging.info(time.strftime('<%Y-%m-%dT%H:%M:%SZ> ', time.gmtime()) +
                     'Successfully retrieved: ' + jsonResult['name'] + ' id=' + str(id))

    return person


def save_to_csv(animes):
    try:
        df = pd.json_normalize(animes)
        df.to_csv(master, mode='a',
                  index=False, header=False)
    except:
        logging.error(time.strftime('<%Y-%m-%dT%H:%M:%SZ> ',
                                    time.gmtime()) + 'Error while saving person data')


logging.basicConfig(filename='../../mal_person_parser.log', level=logging.INFO)

base_url = 'https://api.jikan.moe/v3'
up_to = get_range()

# Get the starting point
master = '../../dataset/person.csv'
start = 1
try:
    mDf = pd.read_csv(master)
    start = int(mDf.iloc[-1, 0]) + 1
    print(f'Retrieving {up_to} items starting from {str(start)}')
except pd.io.common.EmptyDataError:
    print('Person dataset is empty!')
    start = 1
except IndexError:
    print('Person dataset is empty!')
    start = 1

persons = []

count = 0

for i in range(start, start + up_to):
    person = get_person(i)
    if (person):
        persons.append(person)
        count = count+1

    time.sleep(5)

    if(count % 10 == 0 and count != 0):
        save_to_csv(persons)
        persons = []

save_to_csv(persons)
