import requests as rq
import env_variables as env
import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import execute_values
import cProfile
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_project.log'),
        logging.StreamHandler()
    ]
)


def extract_movies(url: str, data: str, page: str) -> list:
    try:
        header: dict = {
            'Authorization': f'Bearer {env.ACCESS_TOKEN}',
            'accept': 'application/json'
        }
        params: dict = {
            'page': page,
            'sort_by': 'primary_release_date.asc'
        }

        response = rq.request(url=url, method='GET', headers=header, params=params)

        logging.info(f"Api response code : {response.status_code}")

        if str(response.status_code).startswith('20'):
            json_response = response.json()
            results = json_response.get(data)
            total_pages = json_response.get('total_pages', 1)  # Default to 1 if not provided
            return results, total_pages
        else:
            return [], 0
    except Exception as e:
        logging.exception(f'Data extraction error: {e}')


def extract_genre(url: str, data: str):
    try:
        header: dict = {
            'Authorization': f'Bearer {env.ACCESS_TOKEN}',
            'accept': 'application/json'
        }

        response = rq.request(url=url, method='GET', headers=header)

        logging.info(f"Api response code : {response.status_code}")

        return response.json().get(data) if str(response.status_code).startswith('20') else []
    except Exception as e:
        logging.exception(f'Data extraction error: {e}')


def transform_movies(results: list):
    try:
        df = pd.DataFrame(results)
        df.dropna(subset=['id'], inplace=True)
        df.dropna(subset=['genre_ids'], inplace=True)

        movie_genre_df: pd.DataFrame = df.loc[:, ['id', 'genre_ids']]
        movie_genre_df = movie_genre_df.explode(column='genre_ids')
        movie_genre_df = movie_genre_df.rename(columns={'id': 'movie_id', 'genre_ids': 'genre_id'})
        movie_genre_df = movie_genre_df.reset_index(drop=True)
        movie_genre_df.dropna(subset=['genre_id'], inplace=True)

        df["release_date"] = pd.to_datetime(df['release_date'])

        df.drop(columns=['backdrop_path', 'poster_path', 'video', 'adult', 'genre_ids'], inplace=True)

        return df, movie_genre_df
    except Exception as e:
        logging.exception(f'Movie transformation error: {e}')


def transform_genre(results: list) -> pd.DataFrame:
    try:
        genre_df = pd.DataFrame(results)
        genre_df['id'].dropna(inplace=True)

        return genre_df
    except Exception as e:
        logging.exception(f'Genre transformation error: {e}')


def load(data: pd.DataFrame, table_name: str, engine) -> None:
    try:
        data.to_sql(table_name, engine, index=False, if_exists='append', schema='etl_project', method=insert_on_conflict_update)

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


def main():
    engine = create_engine(
        f'postgresql://{env.DB_USER}:{env.DB_PASSWORD}@{env.DB_HOST}:{env.DB_PORT}/{env.DB_NAME}',
        echo=False
    )

    genre_results = extract_genre(env.url.get('genre'), 'genres')

    if genre_results:
        genre = transform_genre(genre_results)
        load(genre, 'genres', engine)

    pagination = 1
    while True:
        logging.debug(f"pagination: {pagination}")

        movie_results, total_pages = extract_movies(env.url.get('movie'), 'results', pagination)
        

        if movie_results:
            movies, movie_genres = transform_movies(movie_results)

            load(movies, 'movies', engine)
            load(movie_genres, 'movie_genres', engine)
            if pagination >= total_pages:
                logging.info("Reached the last page.")
                break

            pagination += 1
    logging.info("ETL process completed.")


if __name__ == "__main__":
    cProfile.run('main()')
