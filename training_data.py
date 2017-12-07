"""
Splits data into train and test.
Takes about 10 seconds.
"""

import sqlite3
import pandas as pd

# Parameters
master_folder = "/media/bigdata/" # Folder containing data / SQL.
train_data_folder = "nmt-chatbot/new_data/" # Folder for *.to and *.from.
table = "parent_reply"            # Name of SQL table.
time_year = "2017"                # Year of data.
time_month = "10"                 # Month of data.
train_test_split = 0.8            # 80% train, 20% test.
timeframes = [[time_year, time_month]]

for timeframe in timeframes:
    connection = sqlite3.connect("RC_{}-{}.db".format(*timeframe))
    c = connection.cursor()

    df = pd.read_sql("SELECT * FROM {} WHERE parent NOT NULL ORDER BY created_utc ASC".format(table), connection)
    df_length = len(df)
    split_index = int(train_test_split * df_length)
    with open(train_data_folder + "train.from", "w", encoding="utf8") as ff, open(train_data_folder + "train.to", "w", encoding="utf8") as tf:
        for index in range(split_index):
            ff.write(df["parent"].values[index]+'\n')
            tf.write(df["body"].values[index]+'\n')
    with open(train_data_folder + "test.to", "w", encoding="utf8") as ff, open(train_data_folder + "test.from", "w", encoding="utf8") as tf:
        for index in range(split_index, df_length):
            ff.write(df["parent"].values[index]+'\n')
            tf.write(df["body"].values[index]+'\n')
