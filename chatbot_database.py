"""
Reads newline-separated JSON data, inserts into SQL database.
Comments without parents are inserted as-is (with parent NULL).
Comments with parents:
    If comment with same parent doesn't exist, insert as-is (with parent data).
    If comment with same parent exists, and current comment greater score,
 	then update comment (with parent data).
"""
# TODO Expected:
# Final SQL insertions on first pass through JSON data.
# Final SQL updates on second pass through JSON data.
# However, SQL updates never finish, only reduce in number. Why?

import sqlite3
from datetime import datetime
from sql_string import SqlString
import json

# Parameters
master_folder = "/media/bigdata/"        # Folder containing data / SQL.
table = "parent_reply"                   # Name of SQL table.
time_year = "2017"                       # Year of data.
time_month = "10"                        # Month of data.
json_data = "RC_{}-{}".format(time_year,
			      time_month)# Name of JSON data.
db = json_data + ".db"                   # Name of SQL database.
score_threshold = 7                      # Ignore comments below this score.
print_rows = 1000000                     # Print rates every 1000000 rows.

# Comment out if in same directory as this Python file.
json_data = master_folder + json_data
#db = master_folder + db

connection = sqlite3.connect(db)
c = connection.cursor()
sql_string = SqlString(table)
sql_transaction = []

def create_table():
    kwargs = {
            "parent_id":"TEXT PRIMARY KEY",
            "parent":"TEXT",
            "comment_id":"TEXT UNIQUE",
            "body":"TEXT",
            "subreddit":"TEXT",
            "created_utc":"INT",
            "score":"INT"}
    sql = sql_string.create(**kwargs).terminate()
    c.execute(*sql)

def format_data(data):
    data = data.replace("\n", " newlinechar ").replace("\r", " newlinechar ").replace("\"", "'")
    return data

def acceptable(data):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == "[deleted]" or data == "[removed]":
        return False
    else:
        return True

def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 10000:
        c.execute("BEGIN TRANSACTION")
        for sql in sql_transaction:
            try:
                c.execute(*sql)
            except:
                pass
        connection.commit()
        sql_transaction = []

def try_except(f, **kwargs):
    """Catches exceptions of functions that return false on failure."""
    try:
        return f(**kwargs)
    except Exception as e:
        print(f, e)
        return False

def find_sql(*args, **kwargs):
    def helper():
        sql = sql_string.read(*args, **kwargs).limit(1).terminate()
        c.execute(*sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    return try_except(helper)

def insert_sql(**kwargs):
    def helper():
        sql = sql_string.insert(**kwargs).terminate()
        transaction_bldr(sql)
    return try_except(helper)

def update_sql(selections, **kwargs):
    def helper():
        sql = sql_string.update(selections, **kwargs).terminate()
        transaction_bldr(sql)
    return try_except(helper)

if __name__ == "__main__":
    import sys
    create_table()
    total_rows = 0
    paired_rows = 0
    updated_rows = 0
    
    with open(json_data, buffering=10000) as f:
        for row in f:
            total_rows += 1
            row = json.loads(row)
            if "score" in row and row["score"]:
                score = int(row["score"])
                body = format_data(row["body"])

                if score >= score_threshold and acceptable(body):
                    parent_id = row['parent_id']
                    parent = find_sql("body", comment_id=parent_id)
                    comment_id = parent_id[:3] + row['id']
                    subreddit = row["subreddit"]
                    created_utc = int(row["created_utc"])
                    kwargs = {
                            "parent_id":parent_id,
                            "parent":parent,
                            "comment_id":comment_id,
                            "body":body,
                            "subreddit":subreddit,
                            "created_utc":created_utc,
                            "score":score}
                    if not parent:
                        del kwargs["parent"]

                    previous_score = find_sql("score", parent_id=parent_id)
                    if previous_score:
                        if score > previous_score:
                            update_sql({"parent_id":parent_id}, **kwargs)
                            updated_rows += 1
                    else:
                        if parent:
                            paired_rows += 1
                        insert_sql(**kwargs)

                if total_rows % print_rows == 0:
                    print('Paired:{}, Updated:{}, Time:{}'.format(paired_rows/total_rows, updated_rows/total_rows, str(datetime.now())))
                    sys.stdout.flush()
                    total_rows = 0
                    paired_rows = 0
                    updated_rows = 0
