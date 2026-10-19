import datetime

import numpy as np
import pandas as pd

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html
from dash.dependencies import Input, Output

import plotly.graph_objs as go
import plotly.io as pio

import yfinance as yf
from sklearn.decomposition import PCA


pio.templates.default = "plotly"

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])


ticker_field = [
    html.Label("Enter Ticker Symbols:"),
    dcc.Input(
        id="ticker-input",
        type="text",
    ),
]

components_field = [
    html.Label("Select Number of Components:"),
    dcc.Dropdown(
        id="component-dropdown",
        options=[{"label": i, "value": i} for i in range(1, 6)],
        value=3,
    ),
]

date_picker_field = [
    html.Label("Select Date Range:"),
    dcc.DatePickerRange(
        id="date-picker",
        start_date=datetime.datetime.now() - datetime.timedelta(365 * 3),
        end_date=datetime.datetime.now(),
        display_format="YYYY-MM-DD",
    ),
]

submit = [
    html.Button("Submit", id="submit-button"),
]


app.layout = dbc.Container(
    [
        html.H1("PCA on Stock Returns"),
        dbc.Row([dbc.Col(ticker_field)]),
        dbc.Row([dbc.Col(components_field)]),
        dbc.Row([dbc.Col(date_picker_field)]),
        dbc.Row([dbc.Col(submit)]),
        dbc.Row(
            [
                dbc.Col([dcc.Graph(id="bar-chart")], width=4),
                dbc.Col([dcc.Graph(id="line-chart")], width=4),
            ]
        ),
    ]
)

@app.callback(
    [
        Output("bar-chart", "figure"),
        Output("line-chart", "figure"),
    ],
    [Input("submit-button", "n_clicks")],
    [
        dash.dependencies.State("ticker-input", "value"),
        dash.dependencies.State("component-dropdown", "value"),
        dash.dependencies.State("date-picker", "start_date"),
        dash.dependencies.State("date-picker", "end_date"),
    ],
)
def update_graphs(n_clicks, tickers, n_components, start_date, end_date):
    if not tickers:
        return {}, {}

    tickers = tickers.replace(" ", "").split(",")

    start_date = datetime.datetime.strptime(
        start_date,
        "%Y-%m-%dT%H:%M:%S.%f",
    ).date()
    end_date = datetime.datetime.strptime(
        end_date,
        "%Y-%m-%dT%H:%M:%S.%f",
    ).date()

    data = yf.download(
        tickers,
        start=start_date,
        end=end_date,
    )
    daily_returns = data.pct_change().dropna()

    pca = PCA(n_components=n_components)
    pca.fit(daily_returns)

    explained_var_ratio = pca.explained_variance_ratio_
    cumulative_var_ratio = np.cumsum(explained_var_ratio)

    bar_chart = go.Figure(
        data=[
            go.Bar(
                x=["PC" + str(i + 1) for i in range(n_components)],
                y=explained_var_ratio,
            )
        ],
        layout=go.Layout(
            title="Explained Variance by Component",
            xaxis=dict(title="Principal Component"),
            yaxis=dict(title="Explained Variance"),
        ),
    )

    line_chart = go.Figure(
        data=[
            go.Scatter(
                x=["PC" + str(i + 1) for i in range(n_components)],
                y=cumulative_var_ratio,
                mode="lines+markers",
            )
        ],
        layout=go.Layout(
            title="Cumulative Explained Variance",
            xaxis=dict(title="Principal Component"),
            yaxis=dict(title="Cumulative Explained Variance"),
        ),
    )

    return bar_chart, line_chart


if __name__ == "__main__":
    app.run(debug=True)
