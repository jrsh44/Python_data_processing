import sqlite3
import pandas as pd
from database_support import create_database_if_necessary, DATABASE_PATH


def main():

    create_database_if_necessary()

    print("\n=======================\nSQL query 1:\n",sql_1())
    print("\n=======================\nSQL query 2:\n",sql_2())
    print("\n=======================\nSQL query 3:\n",sql_3())
    print("\n=======================\nSQL query 4:\n",sql_4())
    print("\n=======================\nSQL query 5:\n",sql_5())


def sql_1() -> pd.DataFrame:
    conn = sqlite3.connect(DATABASE_PATH)

    query = pd.read_sql_query(
        """
        SELECT Location, SUM(UpVotes) as TotalUpVotes
        FROM Users
        WHERE Location != ''
        GROUP BY Location
        ORDER BY TotalUpVotes DESC
        LIMIT 10
        """, 
        conn)
    
    conn.close()

    return query


def sql_2() -> pd.DataFrame:
    conn = sqlite3.connect(DATABASE_PATH)

    query = pd.read_sql_query(
        """
        SELECT STRFTIME('%Y', CreationDate) AS Year, STRFTIME('%m', CreationDate) AS Month,
            COUNT(*) AS PostsNumber, MAX(Score) AS MaxScore
        FROM Posts
        WHERE PostTypeId IN (1, 2)
        GROUP BY Year, Month
        HAVING PostsNumber > 1000

        """, 
        conn)
    
    conn.close()

    return query


def sql_3() -> pd.DataFrame:
    conn = sqlite3.connect(DATABASE_PATH)

    query = pd.read_sql_query(
        """
        SELECT Id, DisplayName, TotalViews
        FROM (
                SELECT OwnerUserId, SUM(ViewCount) as TotalViews
                FROM Posts
                WHERE PostTypeId = 1
                GROUP BY OwnerUserId
            ) AS Questions
        JOIN Users
        ON Users.Id = Questions.OwnerUserId
        ORDER BY TotalViews DESC
        LIMIT 10
        """, 
        conn)
    
    conn.close()

    return query


def sql_4() -> pd.DataFrame:
    conn = sqlite3.connect(DATABASE_PATH)

    query = pd.read_sql_query(
        """
        SELECT DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes
        FROM (
                SELECT *
                FROM (
                        SELECT COUNT(*) as AnswersNumber, OwnerUserId
                        FROM Posts
                        WHERE PostTypeId = 2
                        GROUP BY OwnerUserId
                    ) AS Answers
                JOIN
                    (
                        SELECT COUNT(*) as QuestionsNumber, OwnerUserId
                        FROM Posts
                        WHERE PostTypeId = 1
                        GROUP BY OwnerUserId
                    ) AS Questions
                ON Answers.OwnerUserId = Questions.OwnerUserId
                WHERE AnswersNumber > QuestionsNumber
                ORDER BY AnswersNumber DESC
                LIMIT 5
            ) AS PostsCounts
        JOIN Users
        ON PostsCounts.OwnerUserId = Users.Id
        """, 
        conn)
    
    conn.close()

    return query


def sql_5() -> pd.DataFrame:
    conn = sqlite3.connect(DATABASE_PATH)

    query = pd.read_sql_query(
        """
        SELECT Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location
        FROM (
                SELECT Posts.OwnerUserId, Posts.Title, Posts.CommentCount, Posts.ViewCount,
                        CmtTotScr.CommentsTotalScore
                FROM (
                        SELECT PostId, SUM(Score) AS CommentsTotalScore
                        FROM Comments
                        GROUP BY PostId
                    ) AS CmtTotScr
                JOIN Posts ON Posts.Id = CmtTotScr.PostId
                WHERE Posts.PostTypeId=1
            ) AS PostsBestComments
        JOIN Users ON PostsBestComments.OwnerUserId = Users.Id
        ORDER BY CommentsTotalScore DESC
        LIMIT 10
        """, 
        conn)
    
    conn.close()

    return query


if __name__ == "__main__":
    main()

