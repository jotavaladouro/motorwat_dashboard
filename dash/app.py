"""
Create dasp app using Flask
"""
import dash
from flask import Flask


server = Flask(__name__)
app = dash.Dash(__name__, server=server)
app.config.suppress_callback_exceptions = True



