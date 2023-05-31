import pandas as pd
import numpy as np
from database_support import read_Users, read_Comments, read_Posts
from sql_queries import sql_1, sql_2, sql_3, sql_4, sql_5


def main():
    
    Posts = read_Posts()
    Users = read_Users()
    Comments = read_Comments()
    
    print("\n=======================\nPandas task 1:")
    pandas_data = pandas_1(Users=Users)
    print(pandas_data)
    compare_data(sql_data=sql_1(), pandas_data=pandas_data)
    
    # print("\n=======================\nPandas task 2:")
    # pandas_data = pandas_2(Posts=Posts)
    # print(pandas_data)
    # compare_data(sql_data=sql_2(), pandas_data=pandas_data)
    
    # print("\n=======================\nPandas task 3:")
    # pandas_data = pandas_3(Posts=Posts, Users=Users)
    # print(pandas_data)
    # compare_data(sql_data=sql_3(), pandas_data=pandas_data)
    
    # print("\n=======================\nPandas task 4:")
    # pandas_data = pandas_4(Posts=Posts, Users=Users)
    # print(pandas_data)
    # compare_data(sql_data=sql_4(), pandas_data=pandas_data)
    
    # print("\n=======================\nPandas task 5:")
    # pandas_data = pandas_5(Posts=Posts, Users=Users, Comments=Comments)
    # print(pandas_data)
    # compare_data(sql_data=sql_5(), pandas_data=pandas_data)
    

def compare_data(sql_data: pd.DataFrame, pandas_data: pd.DataFrame) -> pd.DataFrame:
    if(sql_data.equals(pandas_data)):
        print("CORRECT DATA")
    else:
        print("INCORRECT DATA !!!")


def pandas_1(Users: pd.DataFrame) -> pd.DataFrame:
    data = Users[["Location", "UpVotes"]]
    data = data[data["Location"] != ""]
    data = data.groupby("Location").agg({"UpVotes" : "sum"}).rename(columns={"UpVotes" : "TotalUpVotes"})
    data = data.sort_values('TotalUpVotes', ascending=False).head(10).reset_index()
    return data


def pandas_2(Posts: pd.DataFrame) -> pd.DataFrame:

    return None


def pandas_3(Posts: pd.DataFrame, Users: pd.DataFrame) -> pd.DataFrame:

    return None


def pandas_4(Posts: pd.DataFrame, Users: pd.DataFrame) -> pd.DataFrame:

    return None


def pandas_5(Posts: pd.DataFrame, Users: pd.DataFrame, Comments: pd.DataFrame) -> pd.DataFrame:

    return None




if __name__ == "__main__":
    main()