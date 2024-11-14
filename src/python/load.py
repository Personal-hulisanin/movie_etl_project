import logging
import pandas as pd
from psycopg2.extras import execute_values


def load(data: pd.DataFrame, table_name: str, engine) -> None:
    try:
        data.to_sql(table_name,
                    engine,
                    index=False,
                    if_exists='append',
                    schema='etl_project',
                    method=insert_on_conflict_update)

        logging.info(f"Data loaded successfully to: {table_name} ✅")
    except Exception as e:
        logging.exception(f'Loading error for {table_name}: {e}')


def insert_on_conflict_update(table, conn, keys, data_iter):

    table_name = f"{table.schema}.{table.name}" if table.schema else table.name

    columns = [f"{key} = EXCLUDED.{key}" for key in keys if key != 'id']    # Exclude 'id' from updates

    if table.name == 'movie_genres':
        insert_stmt = f"""INSERT INTO {table_name} 
                        ({', '.join(keys)}) 
                        VALUES %s 
                        ON CONFLICT (movie_id, genre_id) 
                        DO NOTHING"""

    else:
        insert_stmt = f"""INSERT INTO {table_name} 
                        ({', '.join(keys)}) 
                        VALUES %s 
                        ON CONFLICT (id) 
                        DO UPDATE 
                        SET {', '.join(columns)}"""
    try:
        with conn.connection.cursor() as cur:
            execute_values(cur, insert_stmt, data_iter)
            conn.connection.commit()
    except Exception as e:
        logging.exception(f'insert_on_conflict_update error {e}')
