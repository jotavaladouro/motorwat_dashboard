"""
Create the base web page for DASH
use: python dash/index.py
"""
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from app import app
from apps import app_historical_map, app_realtime_map

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    # Link bar
    html.Div(children=[
                dcc.Link('Historical Map', href='/apps/app_historical_map'),
                dcc.Link('RealTime Map', href='/apps/app_realtime_map')
             ],
             className="link_bar"),
    # Maps and graphics go here
    html.Div(id='page-content')
])


# For managing diferents pages
@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/apps/app_historical_map':
        return app_historical_map.layout
    if pathname == '/apps/app_realtime_map':
        return app_realtime_map.layout
    else:
        return ""


# Load css
app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css',
})


if __name__ == '__main__':
    app.run_server(debug=True)

