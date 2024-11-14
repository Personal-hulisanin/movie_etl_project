-- Create the database
CREATE DATABASE ETL_Project;

-- Create Schema
CREATE SCHEMA IF NOT EXISTS etl_project;

-- Create Movies Table
CREATE TABLE IF NOT EXISTS etl_project.movies (
    id INTEGER PRIMARY KEY NOT NULL,
    original_language VARCHAR(10),
    original_title VARCHAR(255),
    overview TEXT,
    popularity FLOAT,
    release_date DATE,
    title VARCHAR(255) NOT NULL,
    vote_average FLOAT,
    vote_count INTEGER
);


-- Create Genres Table
CREATE  TABLE IF NOT EXISTS etl_project.genres (
	id INTEGER PRIMARY KEY,
	name VARCHAR(255) NOT NULL UNIQUE
);

-- Create Movie Genres Junction Table
CREATE TABLE IF NOT EXISTS etl_project.movie_genres (
    movie_id INTEGER NOT NULL,
    genre_id INTEGER NOT NULL,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES etl_project.movies(id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES etl_project.genres(id) ON DELETE CASCADE
);
