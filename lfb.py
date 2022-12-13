#import
from bs4 import BeautifulSoup
import requests
import pandas as pd
import datetime

#définition des constantes
base_url_calendar = 'https://basketlfb.com/calendrier/'
base_url_lfb = 'https://basketlfb.com'
base_url_team = 'https://basketlfb.com/equipe/'
base_url_fiba = 'https://livestats.dcd.shared.geniussports.com/data/'
base_url_plays = 'https://livestats.dcd.shared.geniussports.com/u/FFBB/'
start_day = 1
start_month = 10
end_day = 31
end_month = 5
data_path = '~/.basket_project/data/'

def fetch_season_games_teams(season):
    """
    input : season as an integer
    
    --------------------
    return : two DataFrames :
        * one containing date, home, away, lfb_game_id
        * with team_name, team_link
    
    """
    start_date = datetime.date(season, start_month,start_day)
    end_date = datetime.date(season+1, end_month,end_day)
    day_date=start_date
    
    games=[]
    teams=[]
    
    while day_date <= end_date :
        games_day, teams_day = fetch_date_infos(day_date)
        games += games_day
        teams += teams_day
        if (day_date + datetime.timedelta(days=-1)).month != day_date.month :
            print(f'Identifying the games of the month {day_date.month}/{day_date.year}.')
        day_date = day_date + datetime.timedelta(days=1)
    
    return pd.DataFrame(games), pd.DataFrame(teams).drop_duplicates()


def fetch_date_infos(date):
    """
    input : date as datetime
    
    --------------------
    return : two lists of dictionnaries :
        * with day, home, away, lfb_game_id
        * with team_name, team_link
    
    """
    date_str= f'{date.year}-{date.month}-{date.day}'
    url = base_url_calendar+date_str
    reponse = requests.get(url, timeout = 10)
    soup = BeautifulSoup(reponse.content, "html.parser")
    
    games_soup = soup.find_all(class_='score win')
    games = []

    for item in games_soup:
        item['title']
        games.append({'date':date,'home':item['title'].split(' vs ')[0],'away':item['title'].split(' vs ')[1],'lfb_game_id':item['href']})
        
    return games


def add_fiba_id(games_df):
    """
    input : games_df as DataFrame containing a 'lfb_game_id' column  
    
    --------------------
    return : games_df as DataFrame with an extra column 'fiba_game_id'
    
    """
    date_prev = datetime.date(1900,7,1)
    
    games_df['fiba_game_id']=''
    
    for index, row in games_df.iterrows():
        date_game = games_df.iloc[index]['date']
        if date_prev.month != date_game.month or date_prev.year != date_game.year :
            print(f'Fetching the FIBA Game ID for the games of the month {date_game.month}/{date_game.year}.')
        url_game = base_url_lfb + games_df.iloc[index]['lfb_game_id']
        reponse = requests.get(url_game, timeout = 10)
        soup = BeautifulSoup(reponse.content, "html.parser")
        if soup.find(class_='livestats__link') != None:
            fiba_game_id = base_url_fiba + soup.find(class_='livestats__link')['href'][-8:] + 'data.json'
            games_df.iloc[index]['fiba_game_id']=fiba_game_id
            
        date_prev=date_game
        
    return games_df


def fetch_players(teams_df,season):
    
    """
    input : 
        * teams_df as DataFrame containing a 'link' column  
        * year as an int

    --------------------
    return : enriched team_df with list of players

    """  
     
    teams_df['players_list'] = ''
    season_str = f'/{season}'
      
    for i, row in teams_df.iterrows():
        url = base_url_lfb + teams_df.iloc[i]['link']+season_str
        reponse = requests.get(url, timeout = 10)
        soup = BeautifulSoup(reponse.content, "html.parser")
        players_soup = soup.find_all(class_='stats-player')
        players = []

        for item in players_soup:
            players.append(item['title'])
            
        teams_df.iloc[i]['players_list'] = list(set(players))  
        
    return teams_df 
