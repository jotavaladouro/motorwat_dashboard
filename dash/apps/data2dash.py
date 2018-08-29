"""
Transform data in dash componets
"""
import dash_core_components as dcc


def get_graph_general(imh_data):
    """
    Plot data from toll view
    :param imh_data:  Data to use in graph
    :return: dcc.Graph
    """
    return get_graph_imh(imh_data.get_imh_all(),
                         imh_data.get_dates(),
                         "TOLL AHT")


def add_path(grapth_list,
             origin,
             destination,
             imh_data,
             only_time=False,
             only_imh=False):
    """
    Add to grapth list 2 grapth with data from a path: IMH and travel time
    :param grapth_list: List to add the grapth
    :param origin: .- Path origin
    :param destination: Path Destination
    :param imh_data: IMH data
    :param only_time: Only travel time info
    :param only_imh: Only imh info
    :return: grapth_list with the grapth addeed
    """
    # Create title part
    title = ""
    if origin is not None:
        title = title + "from " + str(origin)
    if destination is not None:
        title = title + " to " + str(destination)

    if not only_time:
        series = imh_data.get_imh_path(origin, destination)
        grapth_list.append(get_graph_imh(series,
                                         imh_data.get_dates(),
                                         "AHT " + title))
    if not only_imh:
        series_time = imh_data.get_time_travel_path(origin, destination)
        grapth_list.append(get_graph_time_travel(series_time,
                                                 imh_data.get_dates(),
                                                 "Time " + title))
    return grapth_list


def get_data_from_serie(serie, date):
    """
    :param serie:
    :param date:
    :return:dictionary from a serie to use in dash
    """
    return {'x': serie.index,
            'y': serie,
            'type': 'scatter',
            'name': date}


def get_graph_imh(series, dates, title):
    """
    :param series: Series date with IMH data for a dates
    :param dates: Dates using in series
    :param title:  Title to use in grapth
    :return:  imh dcc graph
    """
    data = []
    for imh_serie, date in zip(series, dates):
        data.append(get_data_from_serie(imh_serie, date))
    graph = dcc.Graph(
                                id=title,
                                figure={
                                    'data': data,
                                    'layout': {
                                        'title': title
                                        }
                                },
                                config={'displayModeBar': False},
                                className="graph_info"
                        )
    return graph


def get_graph_time_travel(series, dates, title):
    """
    Create a travel time grapth for a path
    :param series: List of time serie for several dates
    :param dates: Dates to use in graprh
    :param title: Title to the grapth
    :return: ddc.Graph Time travel
    """
    data = []
    for serie, date in zip(series, dates):
        data.append(get_data_from_serie(serie, date))
    graph = dcc.Graph(
                                id=title,
                                figure={
                                    'data': data,
                                    'layout': {
                                        'title': title
                                        }
                                },
                                config={'displayModeBar': False},
                                className="graph_info"
                        )
    return graph


def plot_graph_toll(toll, imh_data):
    """
    :param toll: toll info
    :param imh_data: data
    :return: list with 2 dcc.graph: toll imh and travel time for main path
    """
    list_children = [
        get_graph_general(imh_data),
    ]

    add_path(list_children,
             toll["main_source"],
             toll["main_destination"],
             imh_data,
             only_time=True)

    return list_children


def plot_graph_source(path, imh_data):
    """
    :param path: path info
    :param imh_data: data
    :return: list with 2 dcc.graph: path imh and travel time
    """
    list_children = []
    add_path(list_children,
             path["id"],
             None,
             imh_data)
    return list_children


def plot_graph_destination(path, imh_data):
    """
    :param path: path info, only use destination
    :param imh_data: data
    :return: list with  dcc.graph: path destination imh
    """
    list_children = []
    add_path(list_children,
             None,
             path["id"],
             imh_data,
             only_imh=True)
    return list_children


def plot_graph(text_element, toll, imh_data):
    """
    Plot graph for a map element,
    :param text_element: Name map elemento
    :param toll: Toll info
    :param imh_data: data
    :return: List of dcc.Graph
    """
    if text_element == toll["name"]:
        return plot_graph_toll(toll, imh_data)
    for path in toll["source"]:
        if path["name"] == text_element:
            return plot_graph_source(path, imh_data)
    for path in toll["destination"]:
        if path["name"] == text_element:
            return plot_graph_destination(path, imh_data)
    return "Nothing found"
