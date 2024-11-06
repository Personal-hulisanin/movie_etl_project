import pandas as pd
import logging


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
