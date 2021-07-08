import pyarrow
import os.path
import http.client
import pandas as pd
import get_funcs.get_data_funcs as get_data
import get_funcs.get_table_funcs as get_tables
from google.cloud import bigquery
from google.oauth2 import service_account


def load_table(key_json: str, table_name: str, df: pd.DataFrame):
    """
    Adds table to bigquery

    :param key_json: str
    :param table_name: str
    :param df: pd.DataFrame
    :return:
    """

    credentials = service_account.Credentials.from_service_account_file(key_json)
    client = bigquery.Client(credentials=credentials)
    dataset = client.get_dataset('test_dataset')
    table = dataset.table(table_name)
    _ = client.load_table_from_dataframe(df, destination=table)


if __name__ == '__main__':
    host = get_data.HOST
    api_key = get_data.API_KEY
    country = 'Brazil'

    conn = http.client.HTTPSConnection(host)

    headers = {
        'x-rapidapi-host': host,
        'x-rapidapi-key': api_key,
    }

    league_name = get_tables.LEAGUES[country]
    leagues_info = get_data.get_info('leagues', conn, headers, country=country, name=league_name.replace(' ', '%20'))
    league_id = leagues_info[0]['league']['id']

    future_matches_df = get_tables.get_agg_table(country, league_name, league_id, conn, headers)

    team_ids = []
    team_1_ids = future_matches_df['team_1_id'].values
    team_2_ids = future_matches_df['team_2_id'].values
    team_ids.extend(team_1_ids)
    team_ids.extend(team_2_ids)

    teams_df = get_tables.get_teams_statistics_table(team_ids, league_id, conn, headers)
    top_scorers_df = get_tables.get_top_scorers_table(league_id, conn, headers)

    key_json = os.path.join('key', 'project-test-319109-9f750055e96b.json')
    load_table(key_json, 'future_matches', future_matches_df)
    load_table(key_json, 'teams_stats', teams_df)
    load_table(key_json, 'top_scorers', top_scorers_df)
