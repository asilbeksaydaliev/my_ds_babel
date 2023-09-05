import csv
import sqlite3
import pandas as pd

def sql_to_csv(db_name, tab_name):
    conn=sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(f"select * from {tab_name};")
    with open("list_fault_lines.csv", 'w',  encoding="utf-8") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([i[0] for i in cursor.description])
        len = 0
        for i in cursor:
            len +=1
            if len !=100:
                csv_writer.writerow(i)
    conn.commit()
    conn.close()

    with open("list_fault_lines.csv", "r") as csv_f:
        return csv_f.read()[:-1]

def csv_to_sql(csv_content, database, table_name):
    con = sqlite3.connect(database)
    cursor = con.cursor()
    cursor.execute(f'create table {table_name}("Volcano Name", "Country", "Type", "Latitude (dd)", "Longitude (dd)", "Elevation (m)")')
    database = pd.read_csv(csv_content).values
    cursor.executemany(f"INSERT INTO {table_name} VALUES(?, ?, ?, ?, ?, ?)", database)
    con.commit()
    cursor.execute(f"SELECT * FROM {table_name}")
    data = [i[0] for i in cursor.description]
    return data
       
csv_to_sql("list_volcano.csv", 'list_volcanos.db','volcanos')

