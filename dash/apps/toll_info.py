"""
Geospatial info and toll info
"""
from . import env

# Toll example
TOLL_CECEBRE = {
    "name": "Cecebre",
    "lat": 43.283507,
    "long": -8.273746,
    "main_source": 13,
    "main_destination": 1,
    "source": [
        {
            "lat": 42.935824,
            "long": -8.464727,
            "name": "Santiago",
            "id": 13
        },
        {
            "lat": 42.967964,
            "long": -8.437999,
            "name": "Sigueiro",
            "id": 12
        },
        {
            "lat": 43.077676,
            "long": -8.417206,
            "name": "Ordes",
            "id": 11
        },
        {
            "lat": 43.269578,
            "long": -8.233127,
            "name": "Macenda",
            "id": 8
        },
    ],
    "destination": [
        {
            "lat": 43.350514,
            "long": -8.417913,
            "name": "Corunha",
            "id": 1
        },
        {
            "lat": 43.495700,
            "long": -8.196409,
            "name": "Ferrol",
            "id": 5
        }
    ]

}


def get_toll_info():
    return TOLL_CECEBRE


def get_toll_marker(toll):
    """
    :param toll:
    :return: Dictionari info to use in mapbox as marker
    """
    return {
            'lat': [toll['lat']],
            'lon': [toll['long']],
            'text': [toll['name']],
            'name': toll['name'],
            "hoverinfo": "name",
            'type': 'scattermapbox',
            'mode': 'markers',
            'marker': {'symbol': "town-hall",
                       'size': 10}
    }


def get_toll_path(toll, point, color):
    """
    Get into to plot a path in mapbox from toll to point
    :param toll:
    :param point:
    :param color:
    :return: Dictionary info to plot in mapbox a line
    """
    return {
            'lat': [toll['lat'], point['lat']],
            'lon': [toll['long'], point['long']],
            'type': 'scattermapbox',
            'mode': 'lines',
            "hoverinfo": "text",
            'line': {
                'width': 1,
                'color': color
            },
            'text': [toll['name'], point['name']],
            'name': point['name'],
        }


def get_paths_ids(toll):
    """
    :param toll:
    :return: path list: (source, destination)
    """
    return [(a["id"], b["id"]) for a in toll["source"] for b in toll["destination"]]


def get_geo_info(toll):
    """
    List of points and path for a toll to plot in mapbox
    :param toll:
    :return: list of dicctionaries
    """
    data = [get_toll_marker(toll)]
    for path_in in toll['source']:
        data.append(get_toll_path(toll, path_in, 'blue'))
    for path_out in toll['destination']:
        data.append(get_toll_path(toll, path_out, 'orange'))
    return data


def get_layout_map():
    """
    :return: Dictinonary mapbox layout
    """
    return {
            "autosize": True,
            "hovermode": 'closest',
            'mapbox': {
                'center': {
                    'lat': 43.2835,
                    'lon': -8.2737
                },
                'zoom': 8,
                'accesstoken': env.mapbox_token,
                'style': 'dark',
                'bearing': 0,
                'pitch': 0,
            },
            'margin': {'l': 0, 'r': 0, 't': 0, 'b': 0},
            'showlegend': False
        }
