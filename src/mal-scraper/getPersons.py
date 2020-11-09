
import requests
import pandas as pd
import time
import logging


def save_to_csv(animes):
    try:
        df = pd.json_normalize(animes)
        df.to_csv('../../dataset/person.csv', mode='a',
                  index=False, header=False)
    except:
        logging.error(time.strftime('<%Y-%m-%dT%H:%M:%SZ> ',
                                    time.gmtime()) + 'Error while saving anime data')


logging.basicConfig(filename='../../mal_person_parser.log', level=logging.INFO)

base_url = 'https://api.jikan.moe/v3'

# Get the starting point
master = '../../dataset/person.csv'
start = 1
try:
    mDf = pd.read_csv(master)
    start = int(mDf.iloc[-1, 0]) + 1
    print('Start is: ' + str(start))
except pd.io.common.EmptyDataError:
    print('Person dataset is empty!')
    start = 1
except IndexError:
    print('Person dataset is empty!')
    start = 1

persons = []

count = 0

for i in range(start, start + 1):
    apiUrl = f'{base_url}/person/{i}'
    response = requests.get(apiUrl)

    if response.status_code == 200:
        jsonResult = response.json()

        person = {}
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

        persons.append(person)
        count = count+1

        logging.info(time.strftime('<%Y-%m-%dT%H:%M:%SZ> ', time.gmtime()) + 'Successfully retrieved: ' +
                     jsonResult['name'] + ' id=' + str(i))

    elif response.status_code == 404:
        logging.info(time.strftime('<%Y-%m-%dT%H:%M:%SZ> ',
                                   time.gmtime()) + 'Person id=' + str(i) + ' does not exist')
    else:
        logging.info(time.strftime('<%Y-%m-%dT%H:%M:%SZ> ', time.gmtime()) + 'Error while retrieving ' + str(i) +
                     ' with status code ' + str(response.status_code))
    time.sleep(5)

    if(count % 10 == 0 and count != 0):
        save_to_csv(persons)
        persons = []

save_to_csv(persons)
