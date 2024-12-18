from flask import Flask
import dash
from dash import Dash, dcc, html, Input, Output

def create_dash_app002(flask_app):
    dash_app = Dash(
        __name__,
        server=flask_app,
        url_base_pathname='/dashboard/app002/',
        suppress_callback_exceptions=True
    )

    dash_app.layout = html.Div([
        html.H1("New Dash App", style={"textAlign": "center"}),
        dcc.Graph(
            id='example-graph',
            figure={
                'data': [
                    {'x': [1, 2, 3, 4, 5], 'y': [4, 1, 3, 5, 2], 'type': 'line', 'name': 'Line 1'},
                    {'x': [1, 2, 3, 4, 5], 'y': [2, 4, 5, 2, 1], 'type': 'bar', 'name': 'Bar 1'},
                ],
                'layout': {
                    'title': 'Dash Data Visualization'
                }
            }
        )
    ])

    return dash_app
