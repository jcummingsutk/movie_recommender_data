import os

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from .config import get_dev_25M_db_params
import yaml


def update_movies(cur, engine):
    df = pd.read_csv(os.path.join("data", "25M", "movies.csv"))
    cur.execute("DROP TABLE IF EXISTS movies")
    df.to_sql(name="movies", con=engine, if_exists="replace")


def update_links(cur, engine):
    df = pd.read_csv(os.path.join("data", "25M", "links.csv"))
    cur.execute("DROP TABLE IF EXISTS links")
    df.to_sql(name="links", con=engine, if_exists="replace")


def update_tags(cur, engine):
    df = pd.read_csv(os.path.join("data", "25M", "tags.csv"))
    cur.execute("DROP TABLE IF EXISTS tags")
    df.to_sql(name="tags", con=engine, if_exists="replace")


if __name__ == "__main__":
    db_params = get_dev_25M_db_params("config.yaml", "config_secret.yaml")
    with open("parameters.yaml", "r") as fp:
        data_params = yaml.safe_load(fp)

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

    print("updating movies")
    update_movies(cur, engine)

    print("updating links")
    update_links(cur, engine)

    print("updating tags")
    update_tags(cur, engine)
