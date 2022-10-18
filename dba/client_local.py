import psycopg2
from sqlalchemy import create_engine
from psycopg2 import Error
from constants import *
from utils import utils

class DBObject(object):
    def __init__(self):
        self.connection = self.__make_connection__()
        self.engine = self.__make_engine__()

    def __make_connection__(self):
        connection = psycopg2.connect(user=MYUSERNAME,
                                      password=MYPASSWORD,
                                      host=MYHOST,
                                      port=MYPORT,
                                      database=MYDATABASE)
        return connection

    def __make_engine__(self):
        engine = create_engine("postgres://%s:%s@%s:%s/%s" % (MYUSERNAME, MYPASSWORD,
                                                              MYHOST, MYPORT, MYDATABASE))
        return engine

    def createTable(self, query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            cursor.close()
            self.connection.commit()
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            cursor.close()

        return updated_rows

    def dropTable(self, query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            cursor.close()
            self.connection.commit()
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            cursor.close()

        return updated_rows

    # query = insert query
    # data = dict
    def insert(self, query, data):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, data)
            updated_rows = cursor.rowcount
            cursor.close()
            self.connection.commit()
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            cursor.close()

        return updated_rows

    # query = insert query
    # data = [dict]
    def insertMany(self, query, data):
        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, data)
            updated_rows = cursor.rowcount
            cursor.close()
            self.connection.commit()
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            cursor.close()

        return updated_rows

    def fetch(self, query):
        result = None
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            cursor.close()

        return result

    # "SELECT * from \"public\".data_status where \"Feeder\"='%s' AND \"status\"='1'

    def update(self, query):
        updated_rows = None
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            updated_rows = cursor.rowcount
            cursor.close()
            self.connection.commit()
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            cursor.close()

        return updated_rows

    def delete(self, query):
        updated_rows = None
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            updated_rows = cursor.rowcount
            cursor.close()
            self.connection.commit()
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            cursor.close()

        return updated_rows

    def close(self):
        if self.connection:
            self.connection.close()
 
    def upsert_many(self, schema_name, table_name, df, pk_fields, gis = True, on_conflict = 'update'):
        table_exists_flag = self.check_table(schema_name, table_name)
        print(f"table: {table_name}, table_exists_flag:{table_exists_flag}")

        # Creating Table First Time
        if not table_exists_flag:
            if gis:
                df.to_postgis(table_name, self.engine, schema=schema_name, if_exists='append')
            else:
                df.to_sql(table_name, self.engine, schema=schema_name, if_exists='append', index=False)
            # Making Table Editable in QGIS
            # with self.engine.connect() as con:
            #     con.execute('ALTER TABLE %s.%s ADD PRIMARY KEY (%s);' % (schema_name, table_name, pk_fields[0]))
            alter_query = ''
            with self.engine.connect() as con:
                alter_query='ALTER TABLE %s.%s ADD PRIMARY KEY ("%s");'%(schema_name,table_name,pk_fields[0])
                print(alter_query)
                con.execute(alter_query)
        # Upserting if table exists 
        else:
            cursor=self.connection.cursor()
            utils.upsert_multiple_rows(cursor, table_name, pk_fields, schema_name, df, on_conflict)
            cursor.close()
            self.connection.commit()
    
    # def upsert_many_nongis(self, schema_name, table_name, df, pk_fields):
    #     table_exists_flag = self.check_table(schema_name, table_name)
    #     print(f"table_exists_flag:{table_exists_flag}")

    #     # Creating Table First Time
    #     if not table_exists_flag:
    #         df.to_sql(table_name, self.engine, schema=schema_name, if_exists='append')
            
    #         # Making Table Editable in QGIS
    #         alter_query = ''
    #         with self.engine.connect() as con:
    #             alter_query='ALTER TABLE %s.%s ADD PRIMARY KEY ("%s");'%(schema_name,table_name,pk_fields[0])
    #             print(alter_query)
    #             con.execute(alter_query)
    #     # Upserting if table exists 
    #     else:
    #         cursor=self.connection.cursor()
    #         utils.upsert_multiple_rows(cursor, table_name, pk_fields, schema_name, df)
    #         cursor.close()
    #         self.connection.commit()

    def upsert(self, schema_name, table_name, df, pk_fields):
        table_exists_flag = self.check_table(schema_name, table_name)
        print(f"table_exists_flag:{table_exists_flag}")

        # Creating Table First Time
        if not table_exists_flag:
            df.to_postgis(table_name, self.engine, schema=schema_name, if_exists='append')
            # Making Table Editable in QGIS
            # with self.engine.connect() as con:
            #     con.execute('ALTER TABLE %s.%s ADD PRIMARY KEY (%s);' % (schema_name, table_name, pk_fields[0]))
            alter_query = ''
            with self.engine.connect() as con:
                alter_query='ALTER TABLE %s.%s ADD PRIMARY KEY ("%s");'%(schema_name,table_name,pk_fields[0])
                print(alter_query)
                con.execute(alter_query)
        # Upserting if table exists 
        else:
            for i, row in df.iterrows():
                # print(row)
                self.upsert_one(table_name, pk_fields, schema_name, row.to_dict())

    def upsert_one(self, table_name, pk_fields, schema_name, data):
        is_updated, updated_rows = None, None
        try:
            cursor = self.connection.cursor()
            # print(cursor)
            # from ..db_data import upsert_data
            self.upsert_data(cursor, table_name, pk_fields, data, schema_name)
            # print(is_updated)
            # updated_rows = cursor.rowcount
            cursor.close()
            self.connection.commit()
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            cursor.close()

        # return updated_rows

    def upsert_data(self, db_cur, table, pk_fields, data, schema=None):
        assert len(pk_fields) > 0, "There must be at least one field as a primary key"
        if schema:
            rel = '%s.%s' % (schema, table)
        else:
            rel = table
        print(f"rel:{rel}")
        print(db_cur)

        fields, field_placeholders, insert_args = [], [], []
        set_clause_list, set_args = [], []
        for f in data.keys():
            fields.append(f)
            # Params For Insert
            if f == 'geometry':
                field_placeholders.append('ST_GeomFromText(%s,4326)')
                insert_args.append(data[f].wkt)
            else:
                field_placeholders.append('%s')
                insert_args.append(data[f])

            # Params for Update
            if f not in pk_fields:
                set_clause_list.append(f'"{f}"=EXCLUDED."{f}"')

        set_clause = ', '.join(set_clause_list)
        fmt_args = (rel, ', '.join('"{0}"'.format(f) for f in fields), ','.join(field_placeholders), ','.join('"{0}"'.format(f) for f in pk_fields),','.join(set_clause_list))

        print(f"insert_args={insert_args}")
        # print(f"fmt_args={fmt_args}")
        insert_query = "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s" % fmt_args
        print(insert_query)
        db_cur.execute(insert_query, insert_args)

    def check_table(self, schema_name, table_name):
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "select exists(select * from information_schema.tables where table_schema = %s and table_name=%s)",
                (schema_name, table_name,))
            table_exists_flag = cursor.fetchone()[0]
            cursor.close()
            self.connection.commit()
        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            cursor.close()
        return table_exists_flag
