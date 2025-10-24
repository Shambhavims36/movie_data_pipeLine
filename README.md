# Movie Data Pipeline

## Overview
This project is a simple ETL pipeline for the MovieLens dataset with optional enrichment from the OMDb API.
It demonstrates extracting data from CSV files, transforming & enriching the data, and loading it into a MySQL database.

## Project structure
```
movie-data-pipeline/
├── etl.py         # main ETL script
├── schema.sql     # SQL schema to create tables
├── queries.sql    # Analytical SQL queries
└── README.md
```

## Prerequisites
- Python 3.8+
- MySQL server (or compatible)
- MovieLens `movies.csv` and `ratings.csv` placed in the same directory as `etl.py`

## Install Python dependencies
```bash
pip install pandas sqlalchemy requests mysql-connector-python
```

## Setup MySQL
1. Start your MySQL server.
2. Create a database (or let the script create it):
   ```sql
   CREATE DATABASE movie_db;
   ```
3. Set environment variables for the script (recommended):
   ```bash
   export DB_USER=root
   export DB_PASS=your_password
   export DB_HOST=localhost
   export DB_PORT=3306
   export DB_NAME=movie_db
   # Optional: export OMDB_API_KEY=your_key
   ```

## Running the ETL
1. Place `movies.csv` and `ratings.csv` in this folder.
2. Run:
```bash
python etl.py
```
- If you have an OMDb API key, set `OMDB_API_KEY` environment variable before running to enable enrichment.
- The script is idempotent for this assignment: it replaces the `movies` and `ratings` tables.

## Notes & Assumptions
- Movie title parsing extracts year when the title contains it (e.g., "Toy Story (1995)").
- OMDb enrichment is optional; the script will still run if no API key is provided.
- For production: add retries, exponential backoff, caching for API, and a separate genres table for normalization.

## SQL Queries
See `queries.sql` for answers to the assignment questions.

## Next improvements
- Normalize genres into a many-to-many table.
- Cache OMDb responses locally to avoid repeated API calls.
- Add logging, unit tests, and docker-compose to simplify deployment.
