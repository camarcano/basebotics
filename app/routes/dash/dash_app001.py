from flask import Flask, render_template
import dash
from dash import Dash, dcc, html, Input, Output, State, dash_table
import polars as pl
import io
import base64

def create_dash_app(flask_app):
    dash_app = Dash(
        __name__,
        server=flask_app,
        url_base_pathname='/dashboard/app001/',
        suppress_callback_exceptions=True
    )

    dash_app.layout = html.Div([
        html.H1("Player Performance Tracker", style={"textAlign": "center"}),

        # Upload component
        dcc.Upload(
            id="upload-data",
            children=html.Div([
                "Drag and Drop or ",
                html.A("Select a CSV File")
            ]),
            style={
                "width": "100%",
                "height": "60px",
                "lineHeight": "60px",
                "borderWidth": "1px",
                "borderStyle": "dashed",
                "borderRadius": "5px",
                "textAlign": "center",
                "margin": "10px",
            },
            multiple=False
        ),

        dcc.Loading(
            id="loading-table",
            type="default",
            children=html.Div(id="output-table")
        ),

        html.Div([
            html.Label("Select Player:"),
            dcc.Dropdown(id="player-dropdown", placeholder="Select a player"),

            html.Label("Select Metric:"),
            dcc.Dropdown(
                id="metric-dropdown",
                options=[
                    {"label": "Pitch Speed", "value": "RelSpeed"},
                    {"label": "IVB", "value": "InducedVertBreak"},
                ],
                multi=True,
                value=["RelSpeed", "InducedVertBreak"]
            ),

            html.Label("Select Columns for Filtering:"),
            dcc.Dropdown(
                id="column-dropdown",
                placeholder="Select columns to filter",
                multi=True
            ),
        ], style={"width": "50%", "margin": "auto"}),

        html.Div(id="filter-dropdowns", style={"width": "50%", "margin": "auto"}),

        dcc.Loading(
            id="loading-stats",
            type="default",
            children=html.Div(id="stats-output", style={"marginTop": "20px"})
        )
    ])

    @dash_app.callback(
        [Output("output-table", "children"),
         Output("player-dropdown", "options"),
         Output("column-dropdown", "options")],
        [Input("upload-data", "contents")],
        [State("upload-data", "filename")]
    )
    def update_table(contents, filename):
        if contents is None:
            return html.Div("No file uploaded yet."), [], []

        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)
        try:
            df = pl.read_csv(io.StringIO(decoded.decode("utf-8")))
        except Exception as e:
            return html.Div(f"Error reading file: {e}"), [], []

        if "Pitcher" not in df.columns:
            return html.Div("Uploaded file does not contain a 'Pitcher' column."), [], []

        player_options = [{"label": player, "value": player} for player in sorted(df["Pitcher"].unique())]
        column_options = [{"label": col, "value": col} for col in df.columns if col != "Pitcher"]

        table = dash_table.DataTable(
            data=df.to_dicts(),
            columns=[{"name": col, "id": col} for col in df.columns],
            page_size=10,
            style_table={"overflowX": "auto"}
        )
        return html.Div([table]), player_options, column_options

    @dash_app.callback(
        Output("filter-dropdowns", "children"),
        [Input("column-dropdown", "value")],
        [State("upload-data", "contents")]
    )
    def update_filter_dropdowns(selected_columns, contents):
        if not selected_columns or not contents:
            return html.Div()

        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)
        df = pl.read_csv(io.StringIO(decoded.decode("utf-8")))

        dropdowns = []
        for col in selected_columns:
            unique_values = df[col].unique().to_list()
            dropdowns.append(html.Div([
                html.Label(f"Select {col}:"),
                dcc.Dropdown(
                    id=f"filter-{col}-dropdown",
                    options=[{"label": val, "value": val} for val in unique_values],
                    multi=True
                )
            ]))
        return html.Div(dropdowns)

    @dash_app.callback(
        Output("stats-output", "children"),
        [Input("player-dropdown", "value"),
         Input("metric-dropdown", "value")] +
        [Input(f"filter-{col}-dropdown", "value") for col in ["PitchType", "OtherColumn"]],
        [State("upload-data", "contents")]
    )
    def compute_stats(player, metrics, *filters_and_contents):
        contents = filters_and_contents[-1]
        filters = filters_and_contents[:-1]

        if not contents or not player or not metrics:
            return html.Div("Please upload a file, select a player, and metrics.")

        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)
        df = pl.read_csv(io.StringIO(decoded.decode("utf-8")))

        if "Pitcher" not in df.columns:
            return html.Div("Uploaded file does not contain a 'Pitcher' column.")
        player_data = df.filter(pl.col("Pitcher") == player)

        for col, selected_values in zip(["PitchType", "OtherColumn"], filters):
            if selected_values:
                player_data = player_data.filter(pl.col(col).is_in(selected_values))

        if player_data.height == 0:
            return html.Div(f"No data available for the selected filters.")

        metric_labels = {
            "RelSpeed": "Pitch Speed",
            "InducedVertBreak": "IVB"
        }

        stats = []
        for metric in metrics:
            label = metric_labels.get(metric, metric)
            if metric in player_data.columns:
                avg = player_data[metric].mean()
                min_val = player_data[metric].min()
                max_val = player_data[metric].max()
                stats.append(
                    html.Div(f"{label}: Avg = {avg:.2f}, Min = {min_val:.2f}, Max = {max_val:.2f}")
                )
            else:
                stats.append(
                    html.Div(f"{label}: Metric not found in data.")
                )

        return html.Div(stats)

    return dash_app
