# -*- coding: utf-8 -*-
"""
Show the dashboard using dash library,
Select a day and using dash show a toll map and data
"""
import dash
import dash_core_components as dcc
import dash_html_components as html
from .big_query_imh import ImhDataCompare
from datetime import datetime as dt
from app import app
from . import toll_info
from . import data2dash


def get_date_picker():
    """
    Create a data picker in html.
    :return: A html.Div
    """
    return html.Div([
        dcc.DatePickerSingle(
            id='date-picker-map',
            min_date_allowed=dt(2018, 5, 15),
            initial_visible_month=dt.today(),
            date=dt.today()
        ),
        html.Div(id='container-date-picker')
    ])


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
    imh_data = ImhDataCompare(paths, sz_date)
    imh_data.load()
    print("Load data")
    # Get general data
    return data2dash.plot_graph_toll(toll,imh_data)


@app.callback(
    dash.dependencies.Output('DivImhGraphHistoricalMap', 'children'),
    [dash.dependencies.Input('date-picker-map', 'date'),
     dash.dependencies.Input('map', 'clickData')])
def update_output_change_date(date, selection):
    """
    Code for a date selection, with upload_date we
    create the html for the new data.
    The return from upload_date is fill in DivImhGraph.
    Called when date-picker changer or click in map
    :param date: date to show in dashboard
    :param selection: point click in the map
    :return: html code
    """
    if imh_data is not None:
        # Date changed
        if date != imh_data.get_dates()[0]:
            print("upload new date " + date)
            return upload_date(date, working_toll)
    if imh_data is None:
        print("update output select " + date)
        return upload_date(date, working_toll)
    elif selection is not None:
        element_text = selection['points'][0]['text']
        return data2dash.plot_graph(element_text, working_toll,imh_data)


# Init date
DATE = "2018-05-22"
# Toll info
working_toll = toll_info.get_toll_info()
# Store the data, global because if we change the call we do not have to
imh_data = None
# Get info about toll marker and path lines
geo_info = toll_info.get_geo_info(working_toll)
# Create map
fig = dict(data=geo_info, layout=toll_info.get_layout_map())
# Create html base for dashboard
list_children = [html.H1(children='DASHBOARD HISTORICAL MAPS'),
                 get_date_picker(),
                 html.Div(children=[
                        dcc.Graph(
                                id='map',
                                figure=fig,
                                config={'displayModeBar': False})
                        ,
                        html.Div(
                            id="DivImhGraphHistoricalMap",
                            className="div_imh_graph_map"
                        )
                     ],
                     className="full_div_horizontal"
                 )
                 ]
layout = html.Div(children=list_children, className ="full_div")

