import os
import time
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

DB_URI = f"mysql+mysqlconnector://root:tiger@localhost:3306/movie_db"

OMDB_API_KEY = os.getenv('OMDB_API_KEY', '9d9c5c4e')


MOVIE_LIMIT = 500  


def fetch_omdb(title, year=None, api_key=OMDB_API_KEY):
    """Fetch movie details from OMDb by title (and optional year)."""
    if not api_key or api_key == '9d9c5c4e':
        return None  
    params = {'t': title, 'apikey': api_key}
    if year:
        params['y'] = str(year)
    try:
        resp = requests.get('http://www.omdbapi.com/', params=params, timeout=5)
        data = resp.json()
        if data.get('Response') == 'True':
            return data
        else:
            return None
    except requests.RequestException:
        return None

def safe_int(value):
    try:
        return int(value)
    except Exception:
        return None


def run_etl(movies_csv='movies.csv', ratings_csv='ratings.csv'):
    print('Starting ETL...')
  
    movies = pd.read_csv(movies_csv)
    ratings = pd.read_csv(ratings_csv)

    if MOVIE_LIMIT:
        movies = movies.head(MOVIE_LIMIT)

    
    def parse_title_year(orig_title):
        year = None
        title = orig_title
        if orig_title.strip().endswith(')'):
            parts = orig_title.strip().rsplit('(', 1)
            if len(parts) == 2:
                title = parts[0].strip()
                y = parts[1].replace(')', '').strip()
                year = safe_int(y)
        return title, year

    movies[['clean_title', 'year_parsed']] = movies['title'].apply(
        lambda t: pd.Series(parse_title_year(t))
    )

   
    enriched = []
    for idx, row in movies.iterrows():
        title = row['clean_title']
        year = row['year_parsed']
        data = fetch_omdb(title, year)
        if data:
            enriched.append({
                'movieId': row['movie_id'],
                'director': data.get('director'),
                'plot': data.get('plot'),
                'boxOffice': data.get('box_Office'),
            })
        else:
            enriched.append({
                'movieId': row['movie_id'],
                'director': None,
                'plot': None,
                'boxOffice': None,
            })
        
        time.sleep(0.1)

    enriched_df = pd.DataFrame(enriched)

 
    movies = movies.merge(enriched_df, on='movieId', how='left')
    movies.rename(columns={'movieId':'movie_id', 'genres':'genre', 'year_parsed':'year'}, inplace=True)

   
    movies['director'] = movies['director'].fillna('Unknown')
    movies['plot'] = movies['plot'].fillna('')
    movies['boxOffice'] = movies['boxOffice'].fillna('Unknown')
    movies['year'] = movies['year'].fillna(0).astype(int)


    try:
        ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    except Exception:
        
        pass

   
    try:
        engine = create_engine(DB_URI)
        
        try:
            tmp_uri = f"mysql+mysqlconnector://root:tiger@localhost:3306/movie_db/"
            tmp_eng = create_engine(tmp_uri)
            with tmp_eng.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS movie_db")) 
        except Exception:
            pass

        
        movies[['movie_id','clean_title','genre','director','plot','boxOffice','year']].rename(
            columns={'clean_title':'title','boxOffice':'box_office'}
        ).to_sql('movies', con=engine, if_exists='replace', index=False)

        ratings[['userId','movieId','rating','timestamp']].rename(
            columns={'userId':'user_id','movieId':'movie_id'}
        ).to_sql('ratings', con=engine, if_exists='replace', index=False)

        print('Loaded tables: movies, ratings')
    except SQLAlchemyError as e:
        print('Error connecting/loading to database:', e)

if __name__ == '__main__':
    run_etl()
