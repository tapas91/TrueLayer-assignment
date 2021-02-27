"""Transform data and load it into Postgres DB."""

import os
import pandas as pd
import sqlalchemy


def data_transform(*args, **kwargs):

    pg_host = os.environ.get("POSTGRES_HOST")
    pg_port = os.environ.get("POSTGRES_PORT")
    pg_user = os.environ.get("POSTGRES_USER")
    pg_pwd = os.environ.get("POSTGRES_PASSWORD")
    pg_db = os.environ.get("POSTGRES_DATABASE")


    movies_csv = "/usr/local/app/downloads/movies_metadata.csv"
    wiki_csv = "/usr/local/app/downloads/filtered-wiki.csv"
    limit_records = 1000

    movies_df = pd.read_csv(movies_csv)

    #Drop rows which do not have budget
    movies_df = movies_df[movies_df['budget'].apply(lambda x: str(x).isdigit())]

    #Convert type of column from object to int
    movies_df['budget'] = movies_df['budget'].astype(str).astype(int)

    #Drop rows which have either revenue or budget as 0
    movies_df = movies_df.drop(movies_df[(movies_df['revenue'] == 0.0) | (movies_df['budget'] == 0)].index)

    #Compute the ratio for each record
    movies_df['ratio'] = movies_df['revenue'].div(movies_df['budget'])

    filtered_wiki_df = pd.read_csv(wiki_csv)
    filtered_wiki_df = filtered_wiki_df.drop_duplicates(subset=['title'], keep='first')

    merged_df = pd.merge(movies_df, filtered_wiki_df, on="title")


    columns_list = ['adult', 'belongs_to_collection', 'genres', 'homepage', 'id',
                    'imdb_id', 'original_language', 'original_title', 'overview',
                    'poster_path', 'production_countries', 'runtime', 
            'spoken_languages', 'status', 'tagline', 'video', 'vote_average', 
            'vote_count']

    filtered_df = merged_df.drop(columns=list(columns_list))
    filtered_df = filtered_df.sort_values('ratio', axis=0, inplace=False, kind='quicksort', ascending=False).head(limit_records)


    conn = sqlalchemy.create_engine(f"postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_db}")
    conn_obj = conn.connect()

    conn_obj.execute("DROP TABLE IF EXISTS movies_table;")

    filtered_df.to_sql('movies_table', conn_obj)
    conn_obj.close()

    merged_df = merged_df.drop(columns=columns_list)
    merged_df.to_csv("/usr/local/app/downloads/merged.csv", index=False)

    print('Data Transformation Completed!!!')