from flask import Flask
from dash import Dash, dcc, html, Input, Output, dash_table, State, ClientsideFunction
from datetime import datetime
import pandas as pd
from .utils import DHHCalculator

def create_dash_app(flask_app):
    dash_app = Dash(
        __name__,
        server=flask_app,
        url_base_pathname='/dashboard/app001/',
        suppress_callback_exceptions=True
    )

    # Add the JavaScript to the app's assets
    dash_app.clientside_callback(
        """
        function(n_clicks) {
            if (n_clicks > 0) {
                const button = document.getElementById('download-button');
                if (button) {
                    button.style.backgroundColor = '#28a745';
                    setTimeout(() => {
                        button.style.backgroundColor = '#007bff';
                    }, 1000);
                }
            }
            return window.dash_clientside.no_update;
        }
        """,
        Output('download-button', 'style'),
        Input('download-button', 'n_clicks'),
        prevent_initial_call=True
    )

    dash_app.layout = html.Div([
        html.H1("Player Performance Tracker", style={"textAlign": "center"}),

        # Store for the full dataset
        dcc.Store(id='stored-data'),

        # Responsive layout for date picker and download button
        html.Div([
            html.Div([
                dcc.DatePickerRange(
                    id='date-picker-range',
                    start_date=datetime(2023, 5, 1),
                    end_date=datetime(2024, 5, 1),
                    display_format='YYYY-MM-DD',
                    style={'marginRight': '15px'}
                )
            ], style={'flex': '1', 'marginRight': '15px'}),
            
            # Download section
            html.Div([
                html.Button(
                    "Download CSV", 
                    id="download-button", 
                    n_clicks=0,
                    style={
                        "backgroundColor": "#007bff",
                        "color": "white",
                        "border": "none",
                        "padding": "8px 16px",
                        "transition": "background-color 0.3s"
                    }
                ),
            ], style={'flex': '1'})
        ], style={'display': 'flex', 'flexWrap': 'wrap', 'alignItems': 'center', 'marginBottom': '20px'}),

        # BBE Filter section with both slider and number input
        html.Div([
            html.Label("Filter by Min BBE:", style={'marginRight': '10px'}),
            html.Div([
                dcc.Slider(
                    id='bbe-slider',
                    min=0,
                    max=100,  # Will be updated dynamically
                    value=0,
                    marks=None,
                    tooltip={"placement": "bottom", "always_visible": True},
                    updatemode='drag',
                    className='custom-slider'
                ),
            ], style={'flex': '1', 'marginRight': '15px', 'marginLeft': '15px'}),
            dcc.Input(
                id="bbe-input",
                type="number",
                value=0,
                min=0,
                style={'width': '80px', 'marginLeft': '10px'}
            )
        ], style={'display': 'flex', 'alignItems': 'center', 'marginBottom': '20px'}),

        # Table display with loading spinner and records per page selector
        html.Div([
            html.Label("Records per page:"),
            dcc.Dropdown(
                id="page-size-selector",
                options=[
                    {"label": "30", "value": 30},
                    {"label": "60", "value": 60},
                    {"label": "90", "value": 90},
                    {"label": "All", "value": -1}
                ],
                value=30,
                style={'width': '120px', 'marginBottom': '10px'}
            )
        ]),
        dcc.Loading(
            id="loading-table",
            type="default",
            children=html.Div(id="output-table")
        ),

        # Hidden download component
        dcc.Download(id="download-dataframe"),
    ])

    @dash_app.callback(
        [Output('stored-data', 'data'),
         Output('bbe-slider', 'max'),
         Output('bbe-slider', 'marks')],
        [Input("date-picker-range", "start_date"),
         Input("date-picker-range", "end_date")]
    )
    def update_stored_data(start_date, end_date):
        parquet_file = 'savant_2023-03-30_2024-09-30.parquet'
        google_sheet_url = "https://docs.google.com/spreadsheets/d/112FJwhapiSNgxepFJQhJnudk_ub5PI9GBN7DMUKBTjc/export?format=csv"
        db_file_name = "google_sheet.db"
        table_name = "google_sheet"
        output_file = "google_sheet.csv"

        dhh_calculator = DHHCalculator(parquet_file, google_sheet_url, db_file_name, table_name, output_file, "/var/www/basebotics/datab")

        try:
            df = dhh_calculator.process(start_date, end_date, 0)  # Get all data
            
            # Process the data for storage
            columns_to_display = [
                "player_name", "BBE", "DHH%", "Sd(LA)", "LA", "Barrel%",
                "MaxEV", "P95 EV", "P90 EV", "P50 EV", "EV", "AVG Hit Distance"
            ]
            df = df[columns_to_display]
            df.rename(columns={"player_name": "Player", "LA": "AVG LA", "EV": "AVG EV"}, inplace=True)
            
            # Calculate max BBE for slider
            max_bbe = df['BBE'].max()
            
            # Create marks for slider
            marks = {i: str(i) for i in range(0, max_bbe + 1, max(1, max_bbe // 10))}
            
            return df.to_dict('records'), max_bbe, marks
        except Exception as e:
            return [], 100, {0: '0'}

    # Callback to sync slider and input
    @dash_app.callback(
        Output('bbe-slider', 'value'),
        Input('bbe-input', 'value'),
        prevent_initial_call=True
    )
    def update_slider(value):
        return value or 0

    @dash_app.callback(
        Output('bbe-input', 'value'),
        Input('bbe-slider', 'value'),
        prevent_initial_call=True
    )
    def update_input(value):
        return value or 0

    @dash_app.callback(
        Output("output-table", "children"),
        [Input('stored-data', 'data'),
         Input("bbe-slider", "value"),
         Input("page-size-selector", "value")]
    )
    def update_table(data, bbe_filter, page_size):
        if not data:
            return html.Div("No data available")

        df = pd.DataFrame(data)
        
        # Apply BBE filter
        filtered_df = df[df['BBE'] >= (bbe_filter or 0)]

        return dash_table.DataTable(
            data=filtered_df.to_dict('records'),
            columns=[
                {"name": col, "id": col, "type": "numeric", "format": {"specifier": ".2f"}} if col not in ["Player", "BBE"] else {"name": col, "id": col}
                for col in filtered_df.columns
            ],
            page_size=page_size if page_size != -1 else len(filtered_df),
            style_table={'overflowX': 'auto'},
            style_cell={
                'textAlign': 'center',
                'height': 'auto',
                'minWidth': '50px',
                'whiteSpace': 'normal'
            },
            style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'},
            filter_action="native",
            sort_action="native"
        )

    @dash_app.callback(
        Output("download-dataframe", "data"),
        Input("download-button", "n_clicks"),
        [State('stored-data', 'data'),
         State("bbe-slider", "value")],
        prevent_initial_call=True
    )
    def download_data(n_clicks, data, bbe_filter):
        if n_clicks > 0 and data:
            df = pd.DataFrame(data)
            filtered_df = df[df['BBE'] >= (bbe_filter or 0)]
            
            return dict(
                content=filtered_df.to_csv(index=False),
                filename='player_performance.csv'
            )
        
        return None

    return dash_app
