# -*- coding: utf-8 -*-
"""
Show the dashboard using dash library,
Load today and using dash show a toll map and data
Recall data every minute
"""
import dash
import dash_core_components as dcc
import dash_html_components as html
from .big_query_imh import ImhDataCompare
from datetime import datetime as dt
from app import app
from . import toll_info
from . import data2dash


def upload_date(sz_date, toll):
    """
    Recalcule data for the date.
    Return html code to show
    :param sz_date: Date to use
    :param toll: Toll to use
    :return: list html code
    """
    paths = toll_info.get_paths_ids(toll)
    print("upload date" + sz_date)
    global imh_data
    if imh_data is None:
        # First call, load data from previous date and today
        print("upload date" + sz_date)
        imh_data = ImhDataCompare(paths, sz_date)
        imh_data.load()
        print("Load data")
    else:
        # Only load data from today, we have data from previouse day
        imh_data.load_last()


@app.callback(
    dash.dependencies.Output('DivImhGraphRealTimeMap', 'children'),
    [dash.dependencies.Input('interval-component', 'n_intervals'),
     dash.dependencies.Input('map', 'clickData')])
def update_output_change_date(interval, selection):
    """
    Code call when click on map, on every minute to update date.
    Change DivImhGraphRealTimeMap
    :param interval: number ticks
    :param selection: map selectiion
    :return: html code
    """
    date = dt.today().strftime("%Y-%m-%d")
    print("update output select " + date + " " + str(interval))
    global last_interval
    if last_interval != interval:
        # Every minute upload data
        upload_date(date, working_toll)
    if selection is not None:
        element_text = selection['points'][0]['text']
        return data2dash.plot_graph(element_text, working_toll, imh_data)
    else:
        return data2dash.plot_graph_toll(working_toll,imh_data)


# Toll info
working_toll = toll_info.get_toll_info()
# Store the data, global because if we change the call we do not have to
imh_data = None
# Get info about toll marker and path lines
geo_info = toll_info.get_geo_info(working_toll)
# Create map
fig = dict(data=geo_info, layout=toll_info.get_layout_map())
# Last tick showed
last_interval = -1


# Create html base for dashboard
list_children = [html.H1(children='DASHBOARD REALTIME MAPS'),
                 html.Div(children=[
                        dcc.Graph(
                                id='map',
                                figure=fig,
                                config={'displayModeBar': False})
                        ,
                        html.Div(
                            id="DivImhGraphRealTimeMap",
                            className="div_imh_graph_map"
                        ),
                         dcc.Interval(
                         id='interval-component',
                         interval=60 * 1000,  # in milliseconds
                         n_intervals=0
                        )
                     ],
                     className="full_div_horizontal"
                 )
                 ]
layout = html.Div(children=list_children, className="full_div")
