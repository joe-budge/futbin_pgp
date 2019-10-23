import requests as rq
from bs4 import BeautifulSoup
import pandas as pd
from random import randint
from time import sleep

headers = ({'User-Agent':
            'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})

def initial_scrape(url): # scrapes initial page - used to find number of pages
    r = rq.get(url, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    return soup


def find_pages(soup): # finds total number of pages
    pages_html = soup.select('.page-link')
    totalPages = pages_html[-2].get_text()
    return totalPages


def convertUnit(price): # converts price format from string to integer e.g. 100K -> 100000
    if 'K' in price:
        price = price.partition('K')[0]
        return int(float(price) * 1000)
    elif 'M' in price:
        price = price.partition('M')[0]
        return int((float(price) * 1000000))
    else:
        return int(price)


def df_calculations(df): # produces useful columns in the dataframe
    df['goal contributions'] = df['goals'] + df['assists']
    df['price per goal'] = df['price'] / df['goals']
    df['price per assist'] = df['price'] / df['assists']
    df['price per goal contribution'] = df['price'] / df['goal contributions']
    return


def gather_data(totalPages): # gathers the data of each player from each page

    database = []
    playerContainers = []
    pageCounter = 0
    totalCounter = 0

    for page in range(int(totalPages)): # for every page
        url = f'https://www.futbin.com/20/pgp?page={page+1}&league=13&games=1000' # change url accordingly
        r = rq.get(url, headers=headers)
        soup = BeautifulSoup(r.text, 'html.parser')

        playerContainers = soup.select('tbody > tr')[2:] 

        counter = 0

        for player in playerContainers: # for every player

            stats = {} # adds stats to a dictionary (one for each player), which is then added to a list

            values = player.select('.num_td')
            name_position = player.select('.player_name_players_table')

            stats['name'] = name_position[0].get_text().partition('(')[0].strip()
            stats['position'] = name_position[0].get_text().partition('(')[2].strip(')')
            stats['rating'] = int(player.select('span[class*="form rating"]')[0].get_text())
            stats['price'] = convertUnit(player.select('.ps4_color')[0].get_text())
            stats['games'] = int(values[0].get_text().replace(',', ''))
            stats['goals'] = float(values[1].get_text())
            stats['assists'] = float(values[2].get_text())

            database.append(stats)

            counter += 1
            print(f'Player: {counter}/{len(playerContainers)} ; {stats["name"]}') # tracks progress

        totalCounter += counter
        pageCounter += 1
        print(f'Page: {pageCounter}/{totalPages}') # more progress tracking
        sleep(randint(1, 3)) # timeout - can be removed

    return database

first_page_soup = initial_scrape('https://www.futbin.com/20/pgp?page=1&games=1000') # change url accordingly
pages = find_pages(first_page_soup)
final_database = gather_data(pages)

df = pd.DataFrame(final_database) # converts to dataframe
df_calculations(df)


df.to_csv('Futbin - PPG Data.csv') # exports to .csv
