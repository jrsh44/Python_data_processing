import os, os.path
import sqlite3
import pandas as pd

DATABASE = "resource/database.db"


def create_database_if_necessary():
    if not os.path.isfile(DATABASE):
        Posts = read_Posts()
        Comments = read_Comments()
        Users = read_Users()

        conn = sqlite3.connect(DATABASE)
        
        if not check_table_existence("Posts") :      Posts.to_sql("Posts", conn)
        if not check_table_existence("Comments") :   Comments.to_sql("Comments", conn)
        if not check_table_existence("Users") :      Users.to_sql("Users", conn)

        conn.close()


def read_Posts() -> pd.DataFrame:
    return pd.read_csv("resource/Posts.csv.gz", compression = "gzip")


def read_Comments() -> pd.DataFrame:
    return pd.read_csv("resource/Comments.csv.gz", compression = "gzip")


def read_Users() -> pd.DataFrame:
    return pd.read_csv("resource/Users.csv.gz", compression = "gzip")


def check_table_existence(table_name) -> bool:
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    result = cursor.fetchone()
    
    conn.close()
    
    return result is not None
