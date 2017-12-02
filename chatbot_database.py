import sqlite3
from datetime import datetime
from sql_string import SqlString
import json
"""
try:
    import ijson.backends.yajl2_cffi as ijson
except:
    import ijson
"""

master_folder = "/media/bigdata/"
time_year = "2017"
time_month = "10"
json_data = "RC_{}-{}".format(time_year, time_month)
db = "RC_{}-{}.db".format(time_year, time_month)

# Read JSON from master folder.
# Write db to master folder.
# Else, read/write to relative filepath.
json_data = master_folder + json_data
#db = master_folder + db

connection = sqlite3.connect(db)
c = connection.cursor()
table = "parent_reply"
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
    c.execute(sql)

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
    if len(sql_transaction) > 100:
        print("\n".join(sorted(sql_transaction)))
        c.execute("BEGIN TRANSACTION")
        for sql in sql_transaction:
            try:
                c.execute(sql)
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
        c.execute(sql)
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
    row_counter = 0
    paired_rows = 0
    updated_rows = 0

    print(find_sql("score", parent_id="t3_73i73d"))
    
    with open(json_data, buffering=1000) as f:
        for row in f:
            row_counter += 1
            row = json.loads(row)
            if "score" in row and row["score"]:
                score = int(row["score"])
                body = format_data(row["body"])

                if score >= 1 and acceptable(body):
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

                    previous_score = find_sql("score", parent_id=parent_id)
                    if previous_score:
                        if score > previous_score:
                            update_sql({"parent_id":parent_id}, **kwargs)
                            updated_rows += 1
                    else:
                        if parent:
                            paired_rows += 1
                        else:
                            del kwargs["parent"]
                        insert_sql(**kwargs)

                if row_counter % 300000 == 0:
                    print('Read:{}, Paired:{}, Updated:{}, Time:{}'.format(row_counter, paired_rows, updated_rows, str(datetime.now())))
                    sys.stdout.flush()
                    row_counter = 0
                    paired_rows = 0
                    updated_rows = 0
