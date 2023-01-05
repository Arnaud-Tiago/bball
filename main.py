from data import increment_games_df, increment_league_df, load_data, add_json
from params import NB_BATCH
import os

method = 'games'
hard_stop = 577597

def execute():
    print(os.getcwd())
    last_ind = 0
    if method == 'games':  
        while last_ind < hard_stop:
            last_ind = add_json(source='cloud')
            print(f"Scraping batch starting at index {last_ind} out of {hard_stop}")
    else :
        while last_ind < hard_stop:
            lldf = load_data(table_name='last_indices',provenance='cloud').set_index('table')
            last_ind = lldf.loc['league','last_scraped_index']  
            for i in range(NB_BATCH):
                print(f"Scraping batch number {i+1} out of {NB_BATCH}")
                increment_league_df(source='cloud')  


if __name__ == '__main__':
    execute()
