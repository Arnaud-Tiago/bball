import requests
import pandas as pd

useless_plays = ['game', 'period', 'timeout']

def find_possible_plays(games):
    """
    input : games_df as DataFrame containing a 'fiba_game_id' column  
    --------------------
    return : three lists:
        * the possible plays list on the games_df,
        * the qualifiers (giving more infos on the plays)
        * the indexes of games without pbp 
    """
    plays =[]
    qualifiers=[]
    errors = []

    for index,row in games.iterrows():
        url_match_fiba = games.iloc[index]['fiba_game_id']
        if not url_match_fiba =='' and not type(url_match_fiba) == float :
            reponse = requests.get(url_match_fiba, timeout = 10)
            if reponse.status_code == 200 :
                #print(reponse.status_code, url_match_fiba)     
                game_detail = reponse.json()
                if not game_detail['pbp'] == []:
                    pbp = pd.DataFrame(game_detail['pbp'])
                    plays += list(pbp['actionType'].unique())
                    qualifiers += list(pbp['qualifier'])
                    
                else :
                    errors.append(index)
            else :
                errors.append(index)
        else :
            errors.append(index)
            
    fq=[]
    for qualifier in qualifiers:
        if not qualifier ==[]:
            for i in qualifier:
                fq.append(i)

    return list(set(plays) - set(useless_plays)),list(set(fq)), errors 


def fetch_game_json(url):    
    """
    input : a fiba_game_url   
    --------------------
    return : a json file containing the game information
    """    
    reponse = requests.get(url, timeout = 10)
    return reponse.json()


def compile_starting_5(json, team_index):
    """
    input : json file containing game info
    --------------------
    return : two lists containing the starters of each team
    """  
    tm = json['tm']
    s5 = []
    players = []
    str_team = str(team_index)
    for player in tm[str_team]['pl'].keys():
        if tm[str_team]['pl'][player]['starter'] == 1:
            s5.append(tm[str_team]['pl'][player]['firstName'] + ' ' + tm[str_team]['pl'][player]['familyName'])
        players.append(tm[str_team]['pl'][player]['firstName'] + ' ' + tm[str_team]['pl'][player]['familyName'])
    
    return s5, players


def fetch_team_games(games_df,error_indexes, team):
    """
    input : 
        * a games_df with fiba_game_id, 
        * an index error list (from fetch_possible_plays)
        * a team name as a string
    --------------------
    return : game_df entiched with starting_5 and player_list
    """      
    mask = [i or j  for i,j in zip(games_df['home']== team, games_df['away']== team)]
    team_games = games_df[mask]
    
    team_games['starting_5'] = ''
    team_games['players'] = ''
    team_games['all_players'] = ''

    all_pl = []

    for index, row in team_games.iterrows():
        if not index in error_indexes:
            team_index = 1 if row['home'] == team else 2
            url = row['fiba_game_id']
            starting_5, players = compile_starting_5(fetch_game_json(row['fiba_game_id']),team_index)
            all_pl += players
            team_games.loc[index]['starting_5'] = starting_5
            team_games.loc[index]['players'] = players
    
    all_pl = list(set(all_pl))
    for index, row in team_games.iterrows():
        team_games.loc[index]['all_players'] = all_pl
       
    
    return team_games


def fetch_other_games_league(url):
    json = fetch_game_json(url)
    try :
        oth_games = json['othermatches']
    except :
        return pd.DataFrame(columns = ['id','competition'])
    oth_list = []
    for item in oth_games:
        oth_list.append({'id':item['id'], 'competition':item['competitionName']})
        
    return oth_list
