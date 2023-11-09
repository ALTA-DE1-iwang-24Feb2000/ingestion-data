from sqlalchemy import create_engine
import pandas as pd

#pg_engine
def create_pg_engine():
    host = 'localhost'
    database = 'store'
    username = 'postgres'
    pwd = 'pass'
    port_id = 5432

    conn_string = f"postgresql://{username}:{pwd}@{host}:{port_id}/{database}"
    pg_engine = create_engine(conn_string)
    
    return pg_engine

#citus_engine
def create_citus_engine():
    host = 'localhost'
    database = 'store'
    username = 'postgres'
    pwd = 'pass'
    port_id = 15432

    conn_string2 = f"postgresql://{username}:{pwd}@{host}:{port_id}/{database}"
    citus_engine = create_engine(conn_string2)

    return citus_engine


def from_pg():
    pg_engine = create_pg_engine()
    pg_conn = pg_engine.connect()

    query = "select * from products"
    
    df = pd.read_sql_query(query,pg_conn)

    pg_conn.close()
    return(df)

def ingest_to_citus(df):
    citus_engine = create_citus_engine()  
    citus_conn = citus_engine.connect()

    df.to_sql('products', citus_conn, if_exists='replace', index=False)

    citus_conn.close()

if __name__ == "__main__":
    data = from_pg()
    ingest_to_citus(data)


# df = pd.read_sql_table('products', citus_conn )
# print(df)