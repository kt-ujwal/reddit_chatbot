"""
Exports strings that follow DB-API 2.0. https://www.python.org/dev/peps/pep-0249/
Used in executing SQLite3 statements. https://docs.python.org/3/library/sqlite3.html
"""

class SqlString(object):
    """
    Functions modify internal list.
    Terminate function returns list as string.
    """

    def __init__(self, table, sql=[], values=[]):
        """Generate sql with respect to table.
        @param table name of database.
        @param sql list of partial SQL strings.
        """
        self.table = table
        self.sql = list(sql)
        self.values = list(values)

    def terminate(self):
        """Return tuple (string, values)
        Convert list to nonempty string, terminated with semicolon.
        Values is tuple of Python values.
        """
        assert len(self.sql) > 0
        string = "".join(self.sql)
        return string + ";", tuple(self.values)

    def create(self, **kwargs):
        """Create new table with kwargs key : TYPE."""
        sql = []
        sql.append("CREATE TABLE IF NOT EXISTS {} (".format(self.table))
        sql.append(", ".join("{} {}".format(k, v) for k, v in kwargs.items()))
        sql.append(")")
        return SqlString(self.table, self.sql + sql, self.values)

    def read(self, *args, **kwargs):
        """Select args columns of statement matching kwargs."""
        sql = []
        if kwargs:
            sql.append("SELECT ")
            sql.append(", ".join(args))
            sql.append(" FROM {} ".format(self.table))
            sql.append("WHERE " + " AND ".join("{} = ?".format(k) for k in kwargs.keys()))
        return SqlString(self.table, self.sql + sql, self.values + list(kwargs.values()))

    def insert(self, **kwargs):
        """Insert rows matching kwargs."""
        sql = []
        sql.append("INSERT INTO {} (".format(self.table))
        sql.append(", ".join(kwargs.keys()))
        sql.append(") VALUES (")
        sql.append(", ".join("?"*len(kwargs)))
        sql.append(")")
        return SqlString(self.table, self.sql + sql, self.values + list(kwargs.values()))

    def update(self, selections, **kwargs):
        """Update select rows from selection, with information matching kwargs."""
        sql = []
        sql.append("UPDATE {} SET ".format(self.table))
        sql.append(", ".join("{} = ?".format(k) for k in kwargs.keys()))
        sql.append(" WHERE " + " AND ".join("{} = ?".format(k) for k in selections.keys()))
        return SqlString(self.table, self.sql + sql, self.values + list(kwargs.values()) + list(selections.values()))

    def delete(self, **kwargs):
        """Deletes rows matching kwargs."""
        sql = []
        sql.append("DELETE FROM {} ".format(self.table))
        sql.append(" WHERE " + " AND ".join("{} = ?".format(k) for k in kwargs.keys()))
        return SqlString(self.table, self.sql + sql, self.values + list(kwargs.values()))

    def limit(self, limit):
        sql = []
        sql.append(" LIMIT {}".format(limit))
        return SqlString(self.table, self.sql + sql, self.values)

if __name__ == "__main__":
    table = "parent_reply"
    sql_string = SqlString(table)
    parent_id = 't3_73ifvr'
    comment_id = 'dnqith5'
    comment = 'Dieb Lauen dann?'
    created_utc = 1506816380
    score = 68
    subreddit = 'de'
    sql_strings = [
            sql_string.create(**{
                    "parent_id":"TEXT PRIMARY KEY",
                    "parent":"TEXT",
                    "comment_id":"TEXT UNIQUE",
                    "comment":"TEXT",
                    "subreddit":"TEXT",
                    "created_utc":"INT",
                    "score":"INT"}),
            sql_string.read("comment", parent_id=parent_id).limit(1),
            sql_string.read("score", comment_id=parent_id).limit(1),
            sql_string.insert(**{
                "parent_id":parent_id,
                "comment_id":comment_id,
                "comment":comment,
                "subreddit":subreddit,
                "created_utc":created_utc,
                "score":score}),
            sql_string.update({"parent_id":parent_id}, **{
                "parent_id":parent_id,
                "comment_id":comment_id,
                "comment":comment,
                "subreddit":subreddit,
                "created_utc":created_utc,
                "score":score})
            ]
    string_tuples = map(lambda s : s.terminate(), sql_strings)
    print('\n'.join(map(lambda st : st[0] + str(st[1]), string_tuples)))
