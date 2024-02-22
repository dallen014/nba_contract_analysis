from nba_api.stats.endpoints import PlayerCareerStats
from nba_api.stats.static import players
import pandas as pd

def get_contract_data():
    """Reads the raw 'player_contract_data' file and executes the following 
       manipulations:
        - Converts the 'Player' column to lowercase
        - Replaces accented characters with their english versions.
        - Splits the 'Player' column into 'First_Name' and 'Last_Name' columns.
        - Drops the original 'Player' column since we no longer need it.
        - Rename the '2023-24' column to something more representative
        - Drop the extra Reggie Bullock row
        
    Args:
        None

    Returns:
          Dataframe: Player with their associated contract values.
    """

    raw_contracts = pd.read_excel('../data/player_contract_data.xlsx',
                                  header=1, usecols= [1,3])
    raw_contracts['Player'] = raw_contracts['Player'].str.lower()
    accented_to_english = {'ā':'a', 'ć':'c', 'č':'c', 'ģ': 'g', 'ņ': 'n',
                           'ö': 'o', 'ş': 's', 'š': 's', 'ü': 'u', 'ū': 'u'}
    for accented,english in accented_to_english.items():
        raw_contracts['Player'] = raw_contracts['Player'].str.\
            replace(accented, english)
    raw_contracts['First_Name'] = raw_contracts['Player'].str.split(n=1).str[0]
    raw_contracts['Last_Name'] = raw_contracts['Player'].str.split(n=1).str[1]
    raw_contracts = raw_contracts.drop(columns=['Player'])
    contract_df = raw_contracts.rename(columns={'2023-24': 'Current_Contract'})
    contract_df = contract_df.drop(index= 386)

    return contract_df

def get_players_with_ids():
    """Gets NBA players with IDs from NBA_API and executes the following 
       manipulations:
        - Converts the 'First_Name' and 'Last_Name' columns to lowercase.
        - Updates certain player name to allow for joining.
        - converts the PlayerID column to a string and rename

    Args:
        None

    Returns:
         Dataframe: Players with their associated ID number
    """

    nba_players = players.get_players()
    players_with_ids = [{'First_Name': player['first_name'],
                         'Last_Name': player['last_name'],
                         'PlayerID': player['id']} for player in nba_players]
    for player in players_with_ids:
        player['First_Name'] = player['First_Name'].lower()
        player['Last_Name'] = player['Last_Name'].lower()
    id_df = pd.DataFrame(players_with_ids)
    id_df.loc[id_df['PlayerID'] == 202694, 'Last_Name'] = 'morris'
    id_df.loc[id_df['PlayerID'] == 1629057, 'Last_Name'] = 'williams'
    id_df.loc[id_df['PlayerID'] == 1628385, 'Last_Name'] = 'giles'
    id_df.loc[id_df['PlayerID'] == 1630214, 'Last_Name'] = 'tillman sr.'
    id_df.loc[id_df['PlayerID'] == 1628995, 'Last_Name'] = 'knox'
    id_df.loc[id_df['PlayerID'] == 203493, 'Last_Name'] = 'bullock'
    id_df.loc[id_df['PlayerID'] == 1631260, 'First_Name'] = 'a.j.'
    id_df['PlayerID'] = id_df['PlayerID'].astype(str)
    return id_df

def get_player_career_stats(player_id):
    """Gets player performance stats by season.

    Args:
        player_id (string): NBA player unique identifier

    Returns:
        Dataframe: Player performance totals for each season
    """
    career_stats = PlayerCareerStats(player_id=player_id)
    career_stats_data = career_stats.get_data_frames()[0]
    return (career_stats_data)

def get_analysis_df(batch_size,df_ids):
    """It uses the dataframe containing player IDs to pull career stats from
       the NBA_API. 

    Args:
        batch_size (int): A number used to pull stat data in batches
        df_ids (dataframe): A dataframe that contains NBA players ID

    Returns:
        Dataframe: Aggregated player performance stats for their last 5 years.
    """
    all_career_stat = pd.DataFrame(columns= ['PLAYER_ID', 'SEASON_ID', 
                                             'LEAGUE_ID', 'TEAM_ID', 
                                             'TEAM_ABBREVIATION', 'PLAYER_AGE', 
                                             'GP', 'GS', 'MIN', 'FGM', 'FGA', 
                                             'FG_PCT', 'FG3M', 'FG3A', 
                                             'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 
                                             'OREB', 'DREB', 'REB', 'AST', 
                                             'STL','BLK', 'TOV', 'PF', 'PTS'])

    for i in range(0,len(df_ids), batch_size):
        batch_player_ids = df_ids['PlayerID'][i:i + batch_size]
        for player_id in batch_player_ids:
            i_df = get_player_career_stats(player_id)
            all_career_stat = pd.concat([all_career_stat, i_df], 
                                        ignore_index= True)
    all_career_stat['SEASON'] =  pd.to_datetime(all_career_stat['SEASON_ID'],
                                                format='%Y-%y').dt.year  
    all_career_stat = all_career_stat.sort_values(by=['PLAYER_ID', 'SEASON'],
                                                  ascending=[True, False])
    all_career_filtered = all_career_stat[all_career_stat['SEASON'] != 2024]
    all_career_grouped = all_career_filtered.groupby('PLAYER_ID').head(5)

    agg_df = all_career_grouped.groupby('PLAYER_ID').agg({'GP': 'sum', 
                                                          'GS': 'sum',
                                                          'MIN': 'sum', 
                                                          'FGM': 'sum',
                                                          'FGA': 'sum',
                                                          'FG3M': 'sum',
                                                          'FG3A': 'sum',
                                                          'FTM': 'sum', 
                                                          'FTA': 'sum',
                                                          'OREB': 'sum', 
                                                          'DREB': 'sum',
                                                          'AST': 'sum', 
                                                          'STL': 'sum',
                                                          'BLK': 'sum', 
                                                          'TOV': 'sum', 
                                                          'PF': 'sum', 
                                                          'PTS': 'sum'}).reset_index()
    agg_df['PLAYER_ID'] = agg_df['PLAYER_ID'].astype(str)
    
    return agg_df