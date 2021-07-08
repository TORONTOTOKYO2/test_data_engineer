import os
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from fpdf import FPDF
from google.cloud import bigquery
from google.oauth2 import service_account

sql_queries = {
    'future_matches': """
    SELECT match_start_datetime as start_date, team_1, team_2,
    team_1_yellow_cards as yellow_cards_1, team_2_yellow_cards as yellow_cards_2
    FROM test_dataset.future_matches
    """,
    'top_scorers': """
    SELECT ts.name as name, ts.goals as goals, fm.team_1 as team
    FROM test_dataset.future_matches as fm
    JOIN test_dataset.top_scorers as ts on fm.team_1_id = ts.team_id
    UNION DISTINCT 
    SELECT ts.name as name, ts.goals as goals, fm.team_2 as team
    FROM test_dataset.future_matches as fm
    JOIN test_dataset.top_scorers as ts on fm.team_2_id = ts.team_id
    ORDER BY goals DESC
    """,
    'teams_stats': """
    SELECT name, form 
    FROM test_dataset.teams_stats
    ORDER BY wins DESC
    """
}


def get_path(*args) -> str:
    """
    Returns path regardless of os

    :param args: pack of unnamed args
    :return: str
    """

    return os.path.join(*args)


def set_colors(value):
    """
    Кeturns the color value based on the cell value

    :param value: str
    :return: str
    """
    if value == 'W':
        return '#7FFF00'
    elif value == 'L':
        return '#DC143C'
    elif value == 'D':
        return '#00BFFF'
    else:
        return '#FFFFFF'


def get_figure(
        column_width: int or list,
        header_columns: list,
        header_fill_color: str,
        header_font_color: str,
        header_line_color: str,
        cells_data: pd.DataFrame,
        cells_height: int,
        cells_fill_color: str or list,
        cells_font_color: str
) -> go.Figure:
    """
    Returns go.Table

    :param column_width: int or list
    :param header_columns: list
    :param header_fill_color: str
    :param header_font_color: str
    :param header_line_color: str
    :param cells_data: pd.DataFrame
    :param cells_height: int
    :param cells_fill_color: str or list
    :param cells_font_color: str
    :return: go.Figure.
    """

    return go.Figure(
        data=[
            go.Table(
                columnwidth=column_width,
                header=dict(
                    values=header_columns,
                    align='center',
                    fill_color=header_fill_color,
                    font_family='Droid Serif',
                    font_color=header_font_color,
                    font_size=10,
                    line_color=header_line_color
                ),
                cells=dict(
                    height=cells_height,
                    values=cells_data,
                    fill_color=cells_fill_color,
                    font_color=cells_font_color,
                    font_size=10,
                    align='center'
                )
            )
        ]
    )


def create_future_matches_table(query_job: bigquery.query, file_name: str):
    """
    Creates future matches table

    :param query_job: bigquery.query
    :param file_name: str
    :return:
    """

    df = query_job.result().to_dataframe()
    columns = list(map(lambda x: x.replace('_', ' ').upper(), df.columns))
    fig = get_figure(100, columns, '#FFA577', '#F9F9FF', '#FFFFFF', df.T, 40, '#FFFFFF', '#000000')
    fig.write_image(file_name, scale=2)


def create_top_scorers_table(query_job: bigquery.query, file_name: str):
    """
    Creates top scorers table

    :param query_job: bigquery.query
    :param file_name: str
    :return:
    """

    df = query_job.result().to_dataframe()
    columns = list(map(lambda x: x.replace('_', ' ').upper(), df.columns))
    fig = get_figure(100, columns, '#D55448', '#F9F9FF', '#FFFFFF', df.T, 30, '#FFFFFF', '#000000')
    fig.write_image(file_name, scale=2)


def create_team_stats_table(query_job: bigquery.query, file_name: str):
    """
    Creates team stats table

    :param query_job: bigquery.query
    :param file_name: str
    :return:
    """

    df = query_job.result().to_dataframe()
    form_df = df['form'].apply(lambda x: x[-5:]).str.split('', expand=True)
    form_df = form_df.drop([0, 6], axis=1)
    form_df['name'] = df['name']
    form_df = form_df[['name', 1, 2, 3, 4, 5]]
    columns = ['NAME', '', '', 'FORM', '', '']
    colors = []
    for i in range(len(form_df)):
        row = []
        for column in form_df.columns:
            row.append(set_colors(form_df.loc[i][column]))
        colors.append(row)
    fig = get_figure([100, 20], columns, '#896E69', '#F9F9FF', '#896E69', form_df.T, 40, np.array(colors).T, '#000000')
    fig.write_image(file_name, scale=2)


def create_report(
        title: str,
        file_name_1: str,
        file_name_2: str,
        file_name_3: str,
        logo_name: str,
        add_name: str,
        output_file_name: str
):
    """
    Creates report

    :param title: str
    :param file_name_1: str
    :param file_name_2: str
    :param file_name_3: str
    :param logo_name: str
    :param add_name: str
    :param output_file_name: str
    :return:
    """

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font('Arial', size=14, style='I')
    pdf.cell(200, 0, txt=title, ln=1, align='C')
    pdf.image(file_name_1, x=5, y=15, w=200, h=150)
    pdf.image(file_name_2, x=5, y=85, w=200, h=150)
    pdf.image(file_name_3, x=30, y=160, w=150, h=100)
    pdf.image(logo_name, x=185, y=0, w=20, h=20)
    pdf.image(add_name, x=85, y=250, w=50, h=50)
    pdf.output(output_file_name)


if __name__ == '__main__':
    key_json = 'key/project-test-319109-9f750055e96b.json'
    credentials = service_account.Credentials.from_service_account_file(key_json)
    client = bigquery.Client(credentials=credentials)

    future_matches_query = client.query(sql_queries['future_matches'])
    future_matches_path = get_path('pictures', '0_future_matches.jpeg')
    create_future_matches_table(future_matches_query, future_matches_path)

    top_scorers_query = client.query(sql_queries['top_scorers'])
    top_scorers_path = get_path('pictures', '1_top_scorers.jpeg')
    create_top_scorers_table(top_scorers_query, top_scorers_path)

    team_stats_query = client.query(sql_queries['teams_stats'])
    team_stats_path = get_path('pictures', '2_team_stats.jpeg')
    create_team_stats_table(team_stats_query, team_stats_path)

    title = 'Brazil: Serie A'
    output_file_name = get_path('pictures', 'report.pdf')
    logo_name = get_path('pictures', 'add', 'logo.png')
    add_name = get_path('pictures', 'add', 'ron.png')
    create_report(title, future_matches_path, top_scorers_path, team_stats_path, logo_name, add_name, output_file_name)
