import logging
import time

from sqlalchemy import create_engine

import env_variables as env
import extract as et
import load as ld
import transform as tr

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.DEBUG,
    filename='etl_project.log',
)


def main():
    engine = create_engine(
        f'postgresql://{env.DB_USER}:{env.DB_PASSWORD}@{env.DB_HOST}:{env.DB_PORT}/{env.DB_NAME}',
        echo=False
    )

    genre_results = et.extract_genre(env.url.get('genre'), 'genres')

    if genre_results:
        genre = tr.transform_genre(genre_results)
        ld.load(genre, 'genres', engine)

    pagination = 1
    while True:
        logging.debug(f"pagination: {pagination}")

        movie_results, total_pages = et.extract_movies(env.url.get('movie'), 'results', pagination)

        if movie_results:
            movies, movie_genres = tr.transform_movies(movie_results)

            ld.load(movies, 'movies', engine)
            ld.load(movie_genres, 'movie_genres', engine)
            
            if pagination >= total_pages:
                logging.info("Reached the last page.")
                break

            pagination += 1


if __name__ == "__main__":

    start_time = time.time()

    main()

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"ETL process completed successfully in {elapsed_time:.4f}")
    logging.info(f"ETL process completed in {elapsed_time:.4f}")
