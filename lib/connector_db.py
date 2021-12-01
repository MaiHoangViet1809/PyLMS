import traceback
import sys
import sqlite3


class db_sqlite:
    def __init__(self):
        self.db = sqlite3.connect(database='../databases/app.db',
                                  isolation_level=None,
                                  detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.db.execute('pragma journal_mode=wal')

    # define function
    def exec(self, sql_text, param=None):
        try:
            return self.db.execute(sql_text) if param is None else self.db.execute(sql_text, param)
        except sqlite3.Error as er:
            print('SQLite error: %s' % (' '.join(er.args)))
            print("Exception class is: ", er.__class__)
            print('SQLite traceback: ')
            exc_type, exc_value, exc_tb = sys.exc_info()
            print(traceback.format_exception(exc_type, exc_value, exc_tb))
            return None

    def object_exists(self, object_name, object_type):
        # inmemory: sqlite_temp_master
        sql_text = """    SELECT count(name) 
                            FROM sqlite_master
                           WHERE upper(type)='{}' 
                             AND upper(name)='{}' """.format(object_type, object_name)
        for row in self.db.execute(sql_text):
            return True if row[0] == 1 else False

    def columns(self, object_name):
        sql_text = """SELECT * FROM {} """.format(object_name)
        h = self.db.execute(sql_text)
        return next(zip(*h.description))


class database(db_sqlite):
    index_tables_ddl = {'TBL_INDEX_CUSTOMER': 'CREATE TABLE TBL_INDEX_CUSTOMER (CUSTOMER_ID VARCHAR, TOPIC_NAME VARCHAR)',
                        'TBL_INDEX_LOAN': 'CREATE TABLE TBL_INDEX_LOAN (LOAN_ID VARCHAR, TOPIC_NAME VARCHAR)'}
    index_ddl = {'INDEX_CUSTOMER': 'CREATE UNIQUE INDEX INDEX_CUSTOMER ON TBL_INDEX_CUSTOMER (CUSTOMER_ID)',
                 'INDEX_LOAN': 'CREATE UNIQUE INDEX INDEX_LOAN ON TBL_INDEX_LOAN (LOAN_ID)'}

    def __init__(self):
        super().__init__()
        for k in database.index_tables_ddl:
            if not self.object_exists(object_name=k, object_type='TABLE'):
                self.exec(sql_text=database.index_tables_ddl[k])
        for k in database.index_ddl:
            if not self.object_exists(object_name=k, object_type='INDEX'):
                self.exec(sql_text=database.index_ddl[k])


if __name__ == '__main__':
    new = database()
    for g in new.exec('SELECT * FROM sqlite_master'):
        print(g)

