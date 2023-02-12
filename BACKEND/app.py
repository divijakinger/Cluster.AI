# Imports
import pandas as pd
import requests
from flask import Flask, request, redirect, url_for
import mysql.connector
import cluster
from sqlalchemy import create_engine, text
from flask_cors import CORS

# Initializing Flask App
app = Flask(__name__)
CORS(app)
# Creating an SQL lite DB to store the clusters
engine = create_engine("sqlite:///final.db")


# Test Endpoint to make sure the api is running
@app.route("/")
def home():
    return "It is working!"


# This endpoint is to take the excel or csv database input from the user
@app.route("/csv", methods=["POST"])
def get_data_csv():
    file = request.files["fisier"]
    if file:
        filepath = file.filename
        if (filepath.split("."))[-1] == "csv" or (filepath.split("."))[-1] == "xlsx":
            file.save(file.filename)
            input_file_type = (filepath.split("."))[-1]
            if input_file_type == "csv":
                database = pd.read_csv(filepath)
            if input_file_type == "xlsx":
                database = pd.read_excel(filepath)
            database.to_csv("final_data.csv", index=False)
            return redirect(
                "http://127.0.0.1:5500/TRINIT_594092-UF46RV01_DEV04/FRONTEND/dashboard.html"
            )
        else:
            return "Invalid file input"
    return "File received successfully"


# This is the database connection endpoint
@app.route("/connect_db", methods=["POST"])
def db_conn():
    creds = request.json
    tablename = creds["tablename"]
    connection = mysql.connector.connect(
        host=creds["hostname"],
        port=creds["port"],
        user=creds["username"],
        passwd=creds["password"],
        database=creds["dbname"],
    )
    crsr = connection.cursor()
    crsr.execute(f"use {creds['dbname']}")
    crsr.execute(
        f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{creds['dbname']}' AND TABLE_NAME = '{tablename}'"
    )
    headers = [d[0] for d in crsr]
    crsr.execute(f"select * from {tablename}")
    db_data = [d for d in crsr]
    database = pd.DataFrame(data=db_data, columns=headers)
    database.to_csv("final_data.csv", index=False)
    return redirect(
        "http://127.0.0.1:5500/TRINIT_594092-UF46RV01_DEV04/FRONTEND/dashboard.html"
    )


# This endpoint is to connect to get data from API endpoint
@app.route("/api_end", methods=["POST"])
def endpoint_check():
    endpoint = request.json["api_end"]
    resp = requests.get(endpoint)
    data = resp.json()["prices"]
    database = pd.DataFrame(data, columns=["cap", "price"])
    database.to_csv("final_data.csv", index=False)
    return {"status": "success"}


# Now this is the final input from the User (Columns , Rules)
@app.route("/send_constraints", methods=["POST"])
def constraints_send():
    data = request.json
    print(data)
    columns = data["columns"]
    rules = data["rules"]
    columns = columns.split(",")
    columns = [i.strip() for i in columns]
    database = pd.read_csv("final_data.csv")
    (
        labels_list,
        database_clusters,
        cluster_centers,
        piechart,
    ) = cluster.cluster_analyzer(database, columns, rules)
    database_clusters.to_sql("data", engine, if_exists="replace")
    return {
        "labels": labels_list,
        "piechart": piechart,
        "cluster_centers": cluster_centers,
    }


@app.route("/refresh", methods=["POST"])
def ref():
    endpoint_check()
    return constraints_send()


# Query Processing API
@app.route("/cluster_queries", methods=["POST"])
def query_cluster():
    data = request.json
    cluster_number = data["cluster"]
    cluster_query = data["sql"]
    conn = engine.connect()
    if "where" in cluster_query.lower():
        query = text(f"{cluster_query} AND cluster = {cluster_number}")
    else:
        query = text(f"{cluster_query} WHERE cluster = {cluster_number}")
    results = pd.read_sql_query(query, conn)
    result_dict = results.to_dict(orient="records")
    final_result = []
    for d in result_dict:
        temp = []
        for k in d.keys():
            temp.append(d[k])
        final_result.append(temp)
    print(final_result)
    return {"heading": list(results.columns), "data": final_result}


# Finally running the App
if __name__ == "__main__":
    app.run(debug=True)
