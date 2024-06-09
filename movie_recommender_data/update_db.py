import os

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from .config import get_dev_db_params

if __name__ == "__main__":
    params = get_dev_db_params("config.yaml", "config_secret.yaml")
    df = pd.read_csv(os.path.join("data", "ratings.csv"))

    conn = psycopg2.connect(
        host=params["host"],
        database=params["database"],
        user=params["user"],
        password=params["password"],
        port=params["port"],
    )

    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS ratings")

    engine = create_engine(
        f'postgresql://{params["user"]}:{params["password"]}@{params["host"]}:{params["port"]}/{params["database"]}',
        connect_args={"sslmode": "require"},
    )

    df.to_sql(name="ratings", con=engine, if_exists="replace")
