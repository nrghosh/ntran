import argparse
import glob
import logging
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as po
from plotly.subplots import make_subplots

def get_latest_csv(policy: str, directory: str):
    g = os.path.join(directory, f"{policy}*.csv")
    csv_files = glob.glob(g)
    if not csv_files:
        logging.log(logging.WARNING, f"no CSV files found that match {g}")
        return None
    
    return max(csv_files, key=os.path.getmtime)

def convert_duration_to_seconds(value):
    if value.endswith("ms"):
        return float(value[:-2]) / 1000
    elif "m" in value:
        parts = value.split("m")
        parts[1] = parts[1][:-2]
        return float(parts[0]) * 60 + float(parts[1])
    elif value.endswith("s"):
        return float(value[:-1])
    else:
        return float(value)
    
def convert_duration_to_milliseconds(value):
    try:
        if value.endswith('us') or value.endswith('µs'):  # handle microseconds lol
            value = value[:-2]
            return float(value) * 0.001
        elif value.endswith('ms'):  
            return float(value[:-2])
        elif "m" in value:  
            parts = value.split("m")
            parts[1] = parts[1][:-1]  
            return (float(parts[0]) * 60 + float(parts[1])) * 1000
        elif value.endswith('s') and not value.endswith('us') and not value.endswith('µs'):  
            return float(value[:-1]) * 1000
        else:
            return float(value)
    except Exception as e:
        print(f"Error converting value: {value}")
        raise e

def create_neondb_figures(results: str, figures: str):
    policies = [
        "cold-neondb",
        "prewarm-neondb",
    ]
    policy_dfs = []
    for policy in policies:
        csv = get_latest_csv(policy, results)
        if csv:
            policy_dfs.append(pd.read_csv(csv))

    if not policy_dfs:
        logging.log(logging.WARNING, "no results found for neondb policies")
        return

    df = pd.concat(policy_dfs)
    df["Duration (secs)"] = df["Duration"].apply(convert_duration_to_seconds)
    max_duration = df["Duration (secs)"].max()
    del df["Duration"]

    fig = px.scatter(
        df,
        x="TransactionCount",
        y="Duration (secs)",
        color="Policy",
        facet_row="TestCase",
        range_y=(0, max_duration+(max_duration*.1))
    )
    fig.update_layout(
        xaxis_title="Transaction Count",
        width=500,
        height=700,
        legend={
            "xanchor": "left",
            "x": 0.01,
            "yanchor": "top",
            "y": 0.99,
        },
    )

    figure_path = os.path.join(figures, "neondb.png")
    fig.write_image(figure_path)

def create_postgres_figures(results: str, figures: str):
    policy_dfs = []
    policies = ["serial-snapshot"]
    filename = "postgres.png"
    for policy in policies:
        csv = get_latest_csv(policy, results)
        if csv:
            policy_dfs.append(pd.read_csv(csv))

    if not policy_dfs:
        logging.log(logging.WARNING, "no results found for postgres policies")
        return
    
    df = pd.concat(policy_dfs)
    df["Duration (ms)"] = df["Duration"].apply(convert_duration_to_milliseconds)

    fig = make_subplots(rows=4, cols=1)

    for i, testcase in enumerate(["Long Update", "Short Insert", "Select Scan", "Select Join"]):
        curr_df = df[df["TestCase"] == testcase]
        fig.append_trace(
            po.Scatter(
                x=curr_df["TransactionCount"],
                y=curr_df["Duration (ms)"],
                name=testcase,
            ),
            row=i+1,
            col=1
        )

    fig.update_layout(
        xaxis_title="Transaction Count",
        xaxis_title_standoff=450,
        yaxis_title="Duration (ms)",
        width=500,
        height=700,
    )

    figure_path = os.path.join(figures, filename)
    fig.write_image(figure_path)

def create_duckdb_figures(results: str, figures: str):
    policies = [
        "duckdb-parallel",
        "duckdb-serial",
    ]
    filename="duckdb.png"
    policy_dfs = []
    for policy in policies:
        csv = get_latest_csv(policy, results)
        if csv:
            policy_dfs.append(pd.read_csv(csv))

    if not policy_dfs:
        logging.log(logging.WARNING, "no results found for duckdb policies")
        return
    
    df = pd.concat(policy_dfs)
    df["Duration (ms)"] = df["Duration"].apply(convert_duration_to_milliseconds)
    max_duration = df["Duration (ms)"].max()

    fig = px.scatter(
        df,
        x="TransactionCount",
        y="Duration (ms)",
        color="Policy",
        facet_row="TestCase",
        range_y=(0, max_duration+(max_duration*.1))
    )
    fig.update_layout(
        xaxis_title="Transaction Count",
        width=500,
        height=700,
        legend={
            "xanchor": "left",
            "x": 0.01,
            "yanchor": "top",
            "y": 0.99,
        },
    )

    figure_path = os.path.join(figures, filename)
    fig.write_image(figure_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="db systems for llm agents results analyzer",
        description="reads the results directory and crafts figures",
    )
    parser.add_argument("-r", "--results", default="./ntran/results", required=False)
    parser.add_argument("-f", "--figures", default="./ntran/figures", required=False)
    args = parser.parse_args()
    
    """
    neondb experiments take on the order of seconds and
    only support up to 10 concurrent branches (so our transaction
    count is low), whereas duckdb and serialsnapshot support way
    more transactions and take on the order of milliseconds for some tests.
    as a result, we separate out the figures.
    """
    create_neondb_figures(args.results, args.figures)
    create_postgres_figures(args.results, args.figures)
    create_duckdb_figures(args.results, args.figures)
