import os

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from .config import get_dev_db_params
from sklearn.model_selection import train_test_split
import yaml


def get_movie_ids_to_include(df: pd.DataFrame, num_ratings_thresh: int) -> list[int]:
    df_movie_grp = (
        df.groupby("movieId")
        .agg({"userId": len, "rating": "mean"})
        .rename(
            columns={
                "userId": "num_ratings_for_movie",
            }
        )
        .sort_values(by="num_ratings_for_movie")
        .reset_index()
    )
    df_movie_grp_reduced = df_movie_grp[
        df_movie_grp["num_ratings_for_movie"] > num_ratings_thresh
    ]
    return list(df_movie_grp_reduced["movieId"].unique())


def create_train_test_split(
    df: pd.DataFrame, test_size: float
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_train, df_test = train_test_split(
        df, test_size=test_size, random_state=42, stratify=df["rating"].values
    )

    return df_train, df_test


def update_ratings(cur, engine, data_params):
    cur.execute("DROP TABLE IF EXISTS raw_ratings")
    cur.execute("DROP TABLE IF EXISTS ratings")
    cur.execute("DROP TABLE IF EXISTS ratings_train")
    cur.execute("DROP TABLE IF EXISTS ratings_test")
    df = pd.read_csv(os.path.join("data", "ratings.csv"))

    df.to_sql(name="raw_ratings", con=engine, if_exists="replace")

    movie_ids_to_include = get_movie_ids_to_include(df, data_params["movie_thresh"])
    movie_ids_to_include = list(map(lambda x: int(x), movie_ids_to_include))

    print(
        f"{len(movie_ids_to_include)/df['movieId'].unique().shape[0]*100:.2f}% of movies have a greater than 20 ratings"
    )
    df_many_ratings = df[df["movieId"].isin(movie_ids_to_include)]

    df_many_ratings.to_sql(name="ratings", con=engine, if_exists="replace")

    df_train, df_test = create_train_test_split(
        df_many_ratings, data_params["test_frac"]
    )

    df_train.to_sql(name="ratings_train", con=engine, if_exists="replace")
    df_test.to_sql(name="ratings_test", con=engine, if_exists="replace")


def update_movies(cur, engine):
    df = pd.read_csv(os.path.join("data", "movies.csv"))
    cur.execute("DROP TABLE IF EXISTS movies")
    df.to_sql(name="movies", con=engine, if_exists="replace")


def update_links(cur, engine):
    df = pd.read_csv(os.path.join("data", "links.csv"))
    cur.execute("DROP TABLE IF EXISTS links")
    df.to_sql(name="links", con=engine, if_exists="replace")


def update_tags(cur, engine):
    df = pd.read_csv(os.path.join("data", "tags.csv"))
    cur.execute("DROP TABLE IF EXISTS tags")
    df.to_sql(name="tags", con=engine, if_exists="replace")


if __name__ == "__main__":
    db_params = get_dev_db_params("config.yaml", "config_secret.yaml")
    with open("parameters.yaml", "r") as fp:
        data_params = yaml.safe_load(fp)
    df = pd.read_csv(os.path.join("data", "ratings.csv"))

    conn = psycopg2.connect(
        host=db_params["host"],
        database=db_params["database"],
        user=db_params["user"],
        password=db_params["password"],
        port=db_params["port"],
    )

    conn.set_session(autocommit=True)
    cur = conn.cursor()

    engine = create_engine(
        f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}',
        connect_args={"sslmode": "require"},
    )
    print("updating ratings")
    update_ratings(cur, engine, data_params)

    print("updating movies")
    update_movies(cur, engine)

    print("updating links")
    update_links(cur, engine)

    print("updating tags")
    update_tags(cur, engine)
