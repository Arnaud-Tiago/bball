from data import increment_games_df, increment_league_df
from params import NB_BATCH

if __name__ == '__main__':
    for i in range(NB_BATCH):
        print(f"Scraping batch number {i+1} out of {NB_BATCH}")
        increment_league_df(source='cloud')
