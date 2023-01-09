import requests
import pandas as pd

useless_plays = ['game', 'period', 'timeout']
plays_list = ['3pt_made','3pt_missed','2pt_made','2pt_missed','freethrow_made','freethrow_missed','off_rebound','def_rebound','assist','block','steal', 'turnover','foul','foulon']
qualifiers_list = ['shooting','pointsinthepaint','fromturnover','fastbreak']
shoots = ['2pt','3pt','freethrow']
direct_translation = ['foul','foulon', 'assist','steal','turnover', 'block']

def find_errors(games):
    """
    input : games_df as DataFrame containing a 'fiba_game_id' column  
    --------------------
    return : a list containing the indexes of games without pbp 
    """
    errors = []

    for index,row in games.iterrows():
        url_match_fiba = games.iloc[index]['fiba_game_id']
        if not url_match_fiba =='' and not type(url_match_fiba) == float :
            reponse = requests.get(url_match_fiba, timeout = 60)
            if reponse.status_code == 200 :    
                game_detail = reponse.json()
                if game_detail['pbp'] == []:
                    errors.append(index)
            else :
                errors.append(index)
        else :
            errors.append(index)

    return errors 


def fetch_game_json(url):    
    """
    input : a fiba_game_url   
    --------------------
    return : a json file containing the game information
    """    
    reponse = requests.get(url, timeout = 60)
    return reponse.json()


def compile_players(json, team_index):
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
            starting_5, players = compile_players(fetch_game_json(row['fiba_game_id']),team_index)
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
        try :
            oth_list.append({'id':item['id'], 'competition':item['competitionName']})
        except :
            oth_list.append({'id':item['id'], 'competition':'not_found'})
        
    return oth_list


def map_play(row):
    res = ''

    if row['actionType'] in direct_translation:
        res = row['actionType']
    elif row['actionType'] in shoots:
        if row['success'] == 1 :
            res = row['actionType']+'_made'
        else :
            res = row['actionType']+'_missed'
    elif row['actionType'] == 'rebound':
        if row['subType'] == 'defensive':
            res = 'def_rebound'
        else :
            res = 'off_rebound'
    else :
        res = 'ignored'

    return res

def compile_pbp(json):
    pbp = pd.DataFrame(json['pbp']).sort_values('actionNumber', ascending = True)
    pbp.set_index('actionNumber', inplace=True)
    pbp.drop(columns = ['player','shirtNumber','internationalFirstName', 'internationalFamilyName', 'firstNameInitial',
        'familyNameInitial', 'internationalFirstNameInitial',
        'internationalFamilyNameInitial', 'scoreboardName'],inplace = True)
    pbp['player']=pbp['firstName']+ ' ' + pbp['familyName']
    pbp.drop(columns=['firstName','familyName'], inplace = True)    
    
    test =  []
    for index,row in pbp.iterrows():
        test.append(map_play(row))
    pbp['mapped_play'] = test
    
    pbp['poss_time_index'] = pbp['periodType']+'_'+[str(item) for item in pbp['period']]+'_'+pbp['clock']
    
    return pbp
