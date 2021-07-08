import http.client
import pandas as pd
import get_funcs.get_data_funcs as get_data
from requests import get
from bs4 import BeautifulSoup, element


LEAGUES = {
    'Brazil': 'Serie A',
    'Algeria': 'Ligue 1'
}
SEASON = 2021


def get_match_links(league: element.Tag) -> list:
    """
    Returns list of matches href

    :param league: element.Tag
    :return: list
    """

    match_links = []
    for sibling in league.next_siblings:
        if sibling.name == 'h4':
            break

        if sibling.name == 'a':
            match_links.append(sibling)

    return match_links


def get_agg_table(
        country: str,
        league_name: str,
        league_id: int,
        conn: http.client.HTTPSConnection,
        headers: dict
) -> pd.DataFrame:
    """
    Returns a summary table of future matches

    :param country: str (country of competition)
    :param league_name: str
    :param league_id: int
    :param conn: http.client.HTTPSConnection
    :param headers: dict (info about host and api_key)
    :return: pd.DataFrame
    """

    flashscore_url = 'https://m.flashscore.com'

    flashscore_main_html = get(f'{flashscore_url}/?d=0')
    flashscore_main_soup = BeautifulSoup(flashscore_main_html.text, 'html.parser')
    score_data = flashscore_main_soup.select('.soccer>#score-data')[0]
    current_leagues = score_data.find_all('h4')

    league = None
    for i, current_league in enumerate(current_leagues):
        if current_league.string.find(f'{country.upper()}: {league_name.title()}') != -1:
            league = current_league

    if league is None:
        return pd.DataFrame()

    match_links = get_match_links(league)

    future_matches_info = []
    for match_link in match_links:  # много матчей проблемы с производительностью (память + время)
        href = match_link['href']
        flashscore_match_html = get(f'{flashscore_url}{href}')
        flashscore_match_soup = BeautifulSoup(flashscore_match_html.text, 'html.parser')

        team_names = flashscore_match_soup.select('.soccer>h3')[0].string
        team_1, team_2 = team_names.split(' - ')

        match_details = flashscore_match_soup.select('.soccer>.detail')
        # if len(match_details) > 1:  # past matches contain more details (like score or match_status)
        #     continue

        match_start_datetime = match_details[-1].string

        team_info_1 = get_data.get_info('teams', conn, headers, name=team_1.replace(' ', '%20'))
        team_info_2 = get_data.get_info('teams', conn, headers, name=team_2.replace(' ', '%20'))

        if (len(team_info_1) > 0) and (len(team_info_2) > 0):  # it's to do with free version of api
            match_info = {
                'team_1': team_1,
                'team_2': team_2,
                'match_start_datetime': match_start_datetime
            }
            future_matches_info.append(match_info)

        if len(future_matches_info) == 3:
            break

    for match_info in future_matches_info:
        for team in ['team_1', 'team_2']:
            team_name = match_info[team]
            team_info = get_data.get_info('teams', conn, headers, name=team_name.replace(' ', '%20'))[0]

            if team == 'team1':
                stadium = team_info['venue']['name']
                match_info['stadium'] = stadium

            team_id = team_info['team']['id']
            match_info[f'{team}_id'] = team_id

            players_info = get_data.get_info('players', conn, headers, team=team_id, season=SEASON)
            top_scorer_name, top_scorer_goals = get_data.get_top_scorer(players_info)
            yellow_cards = get_data.get_yellow_cards(players_info, team_id)

            match_info[f'{team}_top_scorer_name'] = top_scorer_name
            match_info[f'{team}_top_scorer_goals'] = top_scorer_goals
            match_info[f'{team}_yellow_cards'] = yellow_cards
        match_info['league_id'] = league_id

    return pd.DataFrame(future_matches_info)


def get_teams_statistics_table(
        team_ids: list,
        league_id: int,
        conn: http.client.HTTPSConnection,
        headers: dict) -> pd.DataFrame:
    """
    Return stats about teams with 'team_ids' ids

    :param team_ids: list
    :param league_id: int
    :param conn: http.client.HTTPSConnection
    :param headers: dict (info about host and api_key)
    :return: pd.DataFrame
    """

    teams_info = []
    for team_id in team_ids:
        teams_statistics_info = get_data.get_info(
            'teams/statistics', conn, headers, season=SEASON, team=team_id, league=league_id)
        team_info = {
            'id': team_id,
            'name': teams_statistics_info['team']['name'],
            'form': teams_statistics_info['form'],
            'wins': teams_statistics_info['fixtures']['wins']['total'],
            'loses': teams_statistics_info['fixtures']['loses']['total'],
            'draws': teams_statistics_info['fixtures']['draws']['total'],
        }
        teams_info.append(team_info)

    return pd.DataFrame(teams_info)


def get_top_scorers_table(league_id: int, conn: http.client.HTTPSConnection, headers: dict) -> pd.DataFrame:
    """
    Returns top 10 scorers of league_id last season

    :param league_id: int
    :param conn: http.client.HTTPSConnection
    :param headers: dict (info about host and api_key)
    :return: pd.DataFrame
    """
    top_scorers_info = get_data.get_info('players/topscorers', conn, headers, season=SEASON, league=league_id)
    top_scorers = []
    for i in range(10):
        top_scorer_info = top_scorers_info[i]
        top_scorer = {
            'id': top_scorer_info['player']['id'],
            'name': top_scorer_info['player']['name'],
            'age': top_scorer_info['player']['age'],
            'nationality': top_scorer_info['player']['nationality'],
            'team_id': top_scorer_info['statistics'][0]['team']['id'],
            'goals': top_scorer_info['statistics'][0]['goals']['total']
        }
        top_scorers.append(top_scorer)

    return pd.DataFrame(top_scorers)
