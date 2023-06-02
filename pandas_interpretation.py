import pandas as pd
from database_support import read_Users, read_Comments, read_Posts
from sql_queries import sql_1, sql_2, sql_3, sql_4, sql_5

pd.set_option('mode.chained_assignment', None)

def main():
    
    Posts = read_Posts()
    Users = read_Users()
    Comments = read_Comments()
    
    print("\n=======================\nPandas task 1:")
    pandas_data = pandas_1(Users=Users)
    print(pandas_data)
    compare_data(sql_data=sql_1(), pandas_data=pandas_data)
    
    print("\n=======================\nPandas task 2:")
    pandas_data = pandas_2(Posts=Posts)
    print(pandas_data)
    compare_data(sql_data=sql_2(), pandas_data=pandas_data)
    
    print("\n=======================\nPandas task 3:")
    pandas_data = pandas_3(Posts=Posts, Users=Users)
    print(pandas_data)
    compare_data(sql_data=sql_3(), pandas_data=pandas_data)
    
    print("\n=======================\nPandas task 4:")
    pandas_data = pandas_4(Posts=Posts, Users=Users)
    print(pandas_data)
    compare_data(sql_data=sql_4(), pandas_data=pandas_data)
    
    print("\n=======================\nPandas task 5:")
    pandas_data = pandas_5(Posts=Posts, Users=Users, Comments=Comments)
    print(pandas_data)
    compare_data(sql_data=sql_5(), pandas_data=pandas_data)
    

def compare_data(sql_data: pd.DataFrame, pandas_data: pd.DataFrame) -> pd.DataFrame:
    if(sql_data.equals(pandas_data)):
        print("CORRECT DATA")
    else:
        print("INCORRECT DATA !!!")


def pandas_1(Users: pd.DataFrame) -> pd.DataFrame:

    data = Users[["Location", "UpVotes"]]
    data = data[data["Location"] != ""]
    data = data.groupby("Location").agg(TotalUpVotes=("UpVotes", "sum"))
    data = data.sort_values("TotalUpVotes", ascending=False).head(10).reset_index()

    return data


def pandas_2(Posts: pd.DataFrame) -> pd.DataFrame:

    data = Posts[(Posts["PostTypeId"] == 1) | (Posts["PostTypeId"] == 2)]
    data.loc[:, "Year"] = data["CreationDate"].str.slice(0, 4)
    data.loc[:, "Month"] = data["CreationDate"].str.slice(5, 7)
    data = data.groupby(["Year", "Month"]).agg(PostsNumber=("Score", "size"), MaxScore=("Score", "max"))
    data = data[data["PostsNumber"] > 1000].reset_index()

    return data


def pandas_3(Posts: pd.DataFrame, Users: pd.DataFrame) -> pd.DataFrame:

    questions = Posts[Posts["PostTypeId"] == 1]
    questions = questions.groupby("OwnerUserId").agg(TotalViews=("ViewCount", "sum"))

    data = pd.merge(questions, Users, left_on="OwnerUserId", right_on="Id")
    data = data[["Id", "DisplayName", "TotalViews"]]
    data = data.sort_values("TotalViews", ascending=False).head(10).reset_index(drop=True)

    return data


def pandas_4(Posts: pd.DataFrame, Users: pd.DataFrame) -> pd.DataFrame:

    answers = Posts[Posts["PostTypeId"] == 2]
    answers = answers.groupby("OwnerUserId").agg(AnswersNumber=("OwnerUserId", "count"))

    questions = Posts[Posts["PostTypeId"] == 1]
    questions = questions.groupby("OwnerUserId").agg(QuestionsNumber=("OwnerUserId", "count"))

    posts_counts = pd.merge(answers, questions, on="OwnerUserId")
    posts_counts = posts_counts[posts_counts["AnswersNumber"] > posts_counts["QuestionsNumber"]]
    posts_counts = posts_counts.sort_values("AnswersNumber",  ascending=False).head(5)

    data = pd.merge(Users, posts_counts, left_on="Id", right_on="OwnerUserId")
    data = data[["DisplayName", "QuestionsNumber", "AnswersNumber", "Location", "Reputation", "UpVotes", "DownVotes"]]
    data = data.sort_values("AnswersNumber",  ascending=False).reset_index(drop=True)

    return data


def pandas_5(Posts: pd.DataFrame, Users: pd.DataFrame, Comments: pd.DataFrame) -> pd.DataFrame:

    cmt_tor_scr = Comments.groupby("PostId").agg(CommentsTotalScore=("Score", "sum"))

    posts_best_comments = pd.merge(Posts, cmt_tor_scr, left_on="Id", right_on="PostId")
    posts_best_comments = posts_best_comments[posts_best_comments["PostTypeId"] == 1][["OwnerUserId", "Title", "CommentCount", "ViewCount", "CommentsTotalScore"]]

    data = pd.merge(Users, posts_best_comments, left_on="Id", right_on="OwnerUserId")
    data = data[["Title", "CommentCount", "ViewCount", "CommentsTotalScore", "DisplayName", "Reputation", "Location"]]
    data = data.sort_values("CommentsTotalScore", ascending=False).head(10).reset_index(drop=True)

    return data


if __name__ == "__main__":
    main()