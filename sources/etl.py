from cassandra.cluster import Cluster
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv

from nosql_queries import create_stmts, insert_stmts, select_stmts, drop_stmts


def preprocessing():
    filepath = os.getcwd() + "/event_data"
    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root, "*"))

    full_data_rows_list = []

    for f in file_path_list:
        with open(f, "r", encoding="utf8", newline="") as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)
            for line in csvreader:
                full_data_rows_list.append(line)

    csv.register_dialect("myDialect", quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open("event_datafile_new.csv", "w", encoding="utf8", newline="") as f:
        writer = csv.writer(f, dialect="myDialect")
        writer.writerow(
            [
                "artist",
                "firstName",
                "gender",
                "itemInSession",
                "lastName",
                "length",
                "level",
                "location",
                "sessionId",
                "song",
                "userId",
            ]
        )
        for row in full_data_rows_list:
            if row[0] == "":
                continue
            writer.writerow(
                (
                    row[0],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[12],
                    row[13],
                    row[16],
                )
            )


def init_database(address, dbname):
    cluster = Cluster([address])
    session = cluster.connect()
    keyspace_init = "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = ".format(
        dbname
    )
    keyspace_init = (
        keyspace_init + "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
    )
    session.execute(keyspace_init)
    session.set_keyspace(dbname)
    return cluster, session


def process_query(
    cluster, session, filename, create_stmt, insert_stmt, select_stmt, drop_stmt
):
    session.execute(drop_stmt)
    session.execute(create_stmt)
    with open(filename, encoding="utf8") as f:
        csvreader = csv.reader(f)
        next(csvreader)
        for line in csvreader:
            input_value = tuple(typ(line[index]) for (index, typ) in insert_stmt[1])
            session.execute(insert_stmt[0], input_value)
    try:
        rows = session.execute(select_stmt)
    except Exception as e:
        print(f"Error: {e}")
    print("Result: ")
    for i, row in enumerate(rows):
        output = f"\t{i + 1}) "
        output += ", ".join(str(item) for item in row)
        print(output)


def main():
    preprocessing()
    cluster, session = init_database("127.0.0.1", "sparkify")
    for index in range(0, len(create_stmts)):
        print(f"Processing query {index + 1} . . .")
        process_query(
            cluster,
            session,
            "event_datafile_new.csv",
            create_stmts[index],
            insert_stmts[index],
            select_stmts[index],
            drop_stmts[index],
        )
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
