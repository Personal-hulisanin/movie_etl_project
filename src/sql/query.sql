SELECT id, original_language, original_title, overview, popularity, release_date, title, vote_average, vote_count
FROM etl_project.movies;

SELECT id, name
FROM etl_project.genres;

SELECT movie_id, genre_id
FROM etl_project.movie_genres;

SELECT m.id, m.original_language, m.original_title, m.overview, m.popularity, 
       m.release_date, m.title, m.vote_average, m.vote_count, g.id AS genre_id, g.name AS genre_name
FROM etl_project.movies m
JOIN etl_project.movie_genres mg ON m.id = mg.movie_id
JOIN etl_project.genres g ON mg.genre_id = g.id;


SELECT m.title, g.name 
FROM etl_project.movies m
JOIN etl_project.movie_genres mg ON m.id = mg.movie_id
JOIN etl_project.genres g ON mg.genre_id = g.id;

SELECT m.title, g.name 
FROM etl_project.movies m
JOIN etl_project.movie_genres mg ON m.id = mg.movie_id
JOIN etl_project.genres g ON mg.genre_id = g.id
WHERE g.name = 'Action';
