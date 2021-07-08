import json
import http.client


HOST = 'api-football-v1.p.rapidapi.com'
API_KEY = 'ce3e69133emshf3268fab6d19a92p16ef73jsn9dd0a9b07386'


def get_http_client(value: str, conn: http.client.HTTPSConnection, headers: dict) -> bytes:
    """
    Returns the result of a get_request to the api

    :param value: str (target of request, ex. players, teams/statistics?params=params)
    :param conn: http.client.HTTPSConnection
    :param headers: dict (info about host and api_key)
    :return: bytes
    """

    conn.request('GET', f'/v3/{value}', headers=headers)
    return conn.getresponse().read()


def get_params_path(params: dict) -> str:
    """
    Returns the formatted order of params

    :param params: dict (ex. {'season': 2021, 'team_id': 1})
    :return: str
    """

    path = ''
    if (params is None) or len(params) == 0:
        return path

    for key, value in params.items():
        path += f'{key}={value}&'

    return '?' + path[:-1]


def change_none_to_0(value: int or None) -> int:
    """
    Returns 0 value instead of None

    :param value: int or None
    :return: int(0)
    """

    if value is None:
        return 0
    return value


def get_top_scorer(players_info: list) -> tuple:
    """
    Returns tuple(name, number of goals) for the best scorer in the team

    :param players_info: list (list of dicts, info about players in the team)
    :return: tuple (name, number of goals)
    """

    players_goals = []
    for player_info in players_info:
        player_name = player_info['player']['name']
        statistics = player_info['statistics'][0]
        goals = statistics['goals']['total']
        players_goals.append([player_name, goals])
    players_goals = [(name, change_none_to_0(goals)) for name, goals in players_goals]
    players_goals.sort(key=lambda x: x[1], reverse=True)
    return players_goals[0]


def get_yellow_cards(players_info: dict, team_id: int) -> int:
    """
    Return number of yellow cards received by the team last season

    :param players_info: list
    :param team_id: int
    :return: int
    """

    yellow_cards = 0
    for player_info in players_info:
        statistics = player_info['statistics']
        for stat in statistics:
            if stat['team']['id'] == team_id:
                yellow_cards += change_none_to_0(stat['cards']['yellow'])
    return yellow_cards


def get_info(value, conn, headers, **params) -> dict or list:
    """
    Returns information by 'value' as a dict

    :param value: str (target of request, ex. players, teams/statistics)
    :param conn: http.client.HTTPSConnection
    :param headers: dict (info about host and api_key)
    :param params: dict or None (dict of parameters for the request)
    :return: dict or list (list of dicts)
    """

    req = f'{value}{get_params_path(params)}'
    data = get_http_client(req, conn, headers)
    return json.loads(data)['response']
