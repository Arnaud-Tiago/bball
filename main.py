from data import increment_games_df, increment_league_df, load_data
from params import NB_BATCH
import os

method = 'games'

if __name__ == '__main__':
    print(os.getcwd())
    lldf = load_data(table_name='last_indices',provenance='cloud').set_index('table')
    last_ind = lldf.loc['all_fiba_games','last_scraped_index'] 
    if method == 'games':  
        while last_ind < 2172000:
            lldf = load_data(table_name='last_indices',provenance='cloud').set_index('table')
            last_ind = lldf.loc['all_fiba_games','last_scraped_index']  
            for i in range(NB_BATCH):
                print(f"Scraping batch number {i+1} out of {NB_BATCH}")
                increment_games_df(source='cloud')
    else :
        while last_ind < 2172000:
            lldf = load_data(table_name='last_indices',provenance='cloud').set_index('table')
            last_ind = lldf.loc['league','last_scraped_index']  
            for i in range(NB_BATCH):
                print(f"Scraping batch number {i+1} out of {NB_BATCH}")
                increment_league_df(source='cloud')        


def execute():
    print(os.getcwd())
    lldf = load_data(table_name='last_indices',provenance='cloud').set_index('table')
    last_ind = lldf.loc['all_fiba_games','last_scraped_index'] 
    if method == 'games':  
        while last_ind < 2172000:
            lldf = load_data(table_name='last_indices',provenance='cloud').set_index('table')
            last_ind = lldf.loc['all_fiba_games','last_scraped_index']  
            for i in range(NB_BATCH):
                print(f"Scraping batch number {i+1} out of {NB_BATCH}")
                increment_games_df(source='cloud')
    else :
        while last_ind < 2172000:
            lldf = load_data(table_name='last_indices',provenance='cloud').set_index('table')
            last_ind = lldf.loc['league','last_scraped_index']  
            for i in range(NB_BATCH):
                print(f"Scraping batch number {i+1} out of {NB_BATCH}")
                increment_league_df(source='cloud')  
