import sys


sys.path.insert(0,"/var/www/FlaskApp/FlaskApp/")

from flask import Flask
import sys
from dash import Dash, dcc, html
import dash_bootstrap_components as dbc

from FlaskApp.components.layout1 import create_layout


# server = Flask(__name__)
# app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app = Flask(__name__)

from FlaskApp import data

def create_dash_application(flask_app):
    # ext_style = "https://codepen.io/chriddyp/pen/bWLwgP.css"
    # dash_app = Dash(server=flask_app, external_stylesheets=[ext_style],
    dash_app = Dash(server=flask_app, external_stylesheets=[dbc.themes.DARKLY],
        name="Gateway")

    dash_app.title="Transmissions"
    dash_app.layout=create_layout(dash_app)

    # title_text=dcc.Markdown(children="Hello GEDI mini computer")
    # dash_app.layout=dbc.Container([title_text])

    return dash_app

create_dash_application(app)

if __name__ == "__main__":
    app.run_server()
