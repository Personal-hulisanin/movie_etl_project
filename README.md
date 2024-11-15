# Movie ETL Project

The Movie ETL Project is an Extract, Transform, Load (ETL) pipeline designed to collect, process, and analyze movie data from an external API. This pipeline extracts movie information, cleans and transforms the data, and loads it into a PostgreSQL database, making it ready for analysis and visualization in Power BI.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Data Structure](#data-structure)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Power BI Dashboard](#power-bi-dashboard)

## Overview

This ETL pipeline automates the process of handling movie data, enabling easy retrieval, cleaning, transformation, and storage for further analysis. The data processing ensures consistency and quality, making it suitable for creating visual insights into trends, genres, language distribution, and popularity metrics.

![Project Overview](img/project_overview.png)

## Features

- **Data Extraction**: Retrieves movie data from [The Movie Database (TMDB) API](https://www.themoviedb.org/).
- **Data Transformation**: Cleans and processes raw data for consistency, error handling, and prepares it for analysis.
- **Data Loading**: Loads the transformed data into a PostgreSQL database.
- **Power BI Visualization**: Provides insights into various movie metrics, including trends over time, genre popularity, language distribution, and top movies by popularity.

## Data Structure

The PostgreSQL database consists of the following tables:

1. **movies**: Stores details about each movie, including `id`, `title`, `release_date`, `original_language`, `popularity`, `vote_count`, `vote_average`, and `overview`.
2. **genres**: Stores unique genre types, with fields like `id` and `name`.
3. **movie_genres**: A junction table that links movies to their genres, using foreign keys `movie_id` and `genre_id`.

### DB Schema
  ![db schema](img/DBschema.png)

This structure allows efficient querying of movie details, genre classification, and relationships for analysis and visualization.

## Setup Instructions

### Prerequisites

- Python 3.x
- PostgreSQL
- DBeaver (optional, for database management)
- Power BI (for visualization)
- TMDB API Access Key and Token: Sign up or log in at [TMDB](https://www.themoviedb.org/login) to get an API key.

### Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/Personal-hulisanin/movie_etl_project.git
   cd movie_etl_project

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
3. Set up the PostgreSQL database using the PostgreSQL script [ETL_db_tables_script.sql](./src/ETL_db_tables_script.sql)
.
   
4. Configure the env_variable.py with your access key and token for the api access, and database connection information.
     ```bash
    API_KEY: str = '<api-key>'
    ACCESS_TOKEN: str = '<access-token>'
    
    url = {
        'genre': 'https://api.themoviedb.org/3/genre/movie/list?language=en',
        'movie': 'https://api.themoviedb.org/3/discover/movie'
    }
    
    DB_USER = '<db_user>'
    DB_PASSWORD = '<pass>'
    DB_HOST = 'localhost'
    DB_PORT = 5432
    DB_NAME = 'etl_project'

## Usage

1. Run the ETL pipeline:

    ```bash
    python etl_pipeline.py

  This script will:

  - Extract data from the TMDB API.
  - Transform and clean the data.
  - Load the processed data into the PostgreSQL database

3. Power BI Visualization:

  - Connect Power BI to your PostgreSQL database.
  - Import the data tables and create visualizations based on your analysis needs
  
## Power BI Dashboard
  ![PowerBI Dashboard](img/Powerbi.png)

## Logging Sample
  ![Log Sample](img/log_sample.png)
