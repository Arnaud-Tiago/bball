import pandas as pd
from google.cloud import storage
import requests

from params import LOCAL_DATA_PATH, BUCKET_NAME, BACTH_SIZE
from games import fetch_other_games_league

data_path = LOCAL_DATA_PATH
base_url_fiba = 'https://livestats.dcd.shared.geniussports.com/data/'

def save_data(df:pd.DataFrame, table_name:str, destination='local'):
    """
    Saves a DataFrame df
        * name it with the table_name argument
        * store it in raw folder if clean is False, else store it in clean Folder
        * destination may be 'local' or 'cloud'
    
    """
    
    valid = {'local','cloud'}
    if destination not in valid:
        raise ValueError("Error : destination must be one of %r." % valid)
    
    if destination=="cloud":
        #client = storage.Client.from_service_account_json("basket-369913-0ead522d59d2.json") # use service account credentials
        client = storage.Client()
        export_bucket = client.get_bucket(BUCKET_NAME) #define bucket
        blob = export_bucket.blob(table_name)
        storage.blob._DEFAULT_CHUNKSIZE = 2097152 # 1024 * 1024 B * 2 = 2 MB
        storage.blob._MAX_MULTIPART_SIZE = 2097152 # 2 MB
        blob.upload_from_string(df.to_csv(),"text/csv")
        print(f"Table {table_name} successfully saved on the cloud at {BUCKET_NAME}/{table_name}")


    else:
        path = data_path+table_name+'.csv'
        df.to_csv(path)
        print(f"Table {table_name} successfully saved locally on {path}")  
    
    return None


def load_data(table_name:str, provenance='local') -> pd.DataFrame:
    """
    input :
        * table_name as a str
        * provenance : can be 'local' or 'cloud' 
    ------
    returns : a pd.DataFrance
    """    
    valid = {'local','cloud'}
    if provenance not in valid:
        raise ValueError("Error : destination must be one of %r." % valid)
    
    if provenance == 'local':
        path = data_path+table_name+'.csv'
        df = pd.read_csv(path)
        if 'Unnamed: 0' in df.columns :
            df.drop(columns='Unnamed: 0', inplace=True)
        print(f"Table {table_name} successfully loaded from {path}.")

    else :
        #client = storage.Client.from_service_account_json("basket-369913-0ead522d59d2.json") # use service account credentials
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME) 
        df = pd.read_csv('gs://'+BUCKET_NAME+'/'+table_name)    
        if 'Unnamed: 0' in df.columns :
            df.drop(columns='Unnamed: 0', inplace=True)        
        print(f"Table {table_name} successfully loaded from {BUCKET_NAME}/{table_name}.")
    
    return df
    
    
def scrap_batch(start_index, verbose=True):
    liste = []

    for i in range (start_index, start_index + BACTH_SIZE):
        if (i - start_index) % int(BACTH_SIZE/20) == 0 and verbose:
            print(f'Scraping in progress : {int((i - start_index)/BACTH_SIZE*100)} % completed')
        url = base_url_fiba+str(i)+'/data.json'
        try :
            reponse = requests.get(url, timeout = 60)
        except :
            print(f"ERROR - Request error on the following game : index {i}, url {url}")
        
        if reponse.status_code not in [200, 403, 404] :
            print("ERROR - Code on the following game : index {i}, url {url}") 
        elif reponse.status_code == 200 :
            try :
                json = reponse.json()
            except :
                print(f"ERROR - JSON error on the following game : index {i}, url {url}")
            #print(f"Index : {i}, url : {url}")
            try :
                home = json["tm"]["1"]["name"]
                away = json["tm"]["2"]["name"]
            except :
                home = ""
                away = ""
                print(f"ERROR - No teams found for the following game : index {i}, url {url}")
            liste.append({'id': i, 'url': url,'home':home,'away':away})
    gss = pd.DataFrame(liste)
    
    return gss


def increment_games_df(source='local', verbose=True):
    """
    takes the otiginal dataframe and increment it with all available unseen data
    """
    valid = {'local','cloud'}
    if source not in valid:
        raise ValueError("Error : source must be one of %r." % valid)
    
    df = load_data(table_name= 'all_fiba_games', provenance=source)
    if verbose :
        print(f"Number of lines in the initial DataFrame: {df.shape[0]:_}.".replace('_',' '))
        
    lldf = load_data(table_name='last_indices',provenance=source).set_index('table')
    start_index = lldf.loc['all_fiba_games','last_scraped_index']
    
    if verbose :
        print(f"Scrapping from index  : {start_index:_}.".replace('_',' '))
        print("Work in progress ... This may take a few minutes ...")   
    
    gss = scrap_batch(start_index, verbose)    
    nb_updated = gss.shape[0]

    if verbose:
        print(f"Number of added lines: {nb_updated:_} on {BACTH_SIZE:_} API calls.".replace('_',' '))
    
    df = pd.concat([df,gss]).drop_duplicates()

    if verbose > 0:
        print(f"End of scrapping at index: {start_index+BACTH_SIZE:_}.".replace('_',' '))
        print(f"Number of lines in the DataFrame: {df.shape[0]:_}.".replace('_',' '))
        print("Saving the DataFrame ... This may take a while ...")
        
    save_data(df, table_name= 'all_fiba_games',destination=source)
    
    lldf.loc['all_fiba_games','last_scraped_index'] = start_index + BACTH_SIZE
    save_data(lldf, table_name= 'last_indices',destination=source)
    
    return


def increment_league_df(source = 'local', verbose = True):

    first = False      

    g_df = load_data(table_name= 'all_fiba_games',provenance=source).sort_values('id',ascending=True).reset_index()
    lldf = load_data(table_name='last_indices',provenance=source).set_index('table') 
    if verbose :
        print(f"Number of lines in the Games table : {g_df.shape[0]:_}".replace('_',' '))

    try :
        l_df = load_data(table_name= 'league',provenance=source)
        if verbose :
            print(f"Number of lines in the League table : {l_df.shape[0]:_}".replace('_',' '))  
        start_index = lldf.loc['league','last_scraped_index']              
    except :
        start_index = g_df['id'].min() - 1
        first = True
        if verbose :
            print("Initialization of the League table")

    g_df = g_df[g_df['id']>start_index][:BACTH_SIZE]
    s_ind = g_df['id'].min() 
    e_ind = g_df['id'].max()
        
    if verbose :
        print(f"Number of lines of the Games table to be analyzed : {g_df.shape[0]:_}".replace('_',' '))
        print(f"First considered index : {s_ind:_} // Last considered index : {e_ind:_}".replace('_',' '))
    
    i = 0
    tmp_list = []
    
    for url in g_df['url'] :
        if i % int(BACTH_SIZE / 20) == 0:
            if verbose :
                print(f"Scraping in progress : {int(i/BACTH_SIZE*100)} %")
        
        tmp_list.append(fetch_other_games_league(url))
        i+=1
        
    l_list = []

    for item in tmp_list:
        for dic in item :
            l_list.append(dic)

    if first :
        l_df = pd.DataFrame(l_list).drop_duplicates() 
    else :
        l_df = pd.concat([l_df,pd.DataFrame(l_list)]).drop_duplicates()
    
    if verbose :
        print(f"Number of lines in the League table after incrementation : {l_df.shape[0]:_}".replace('_',' '))

    save_data(l_df,table_name='league',destination=source)
    lldf.loc['league','last_scraped_index'] = e_ind
    save_data(lldf, table_name= 'last_indices',destination=source)

    return 
