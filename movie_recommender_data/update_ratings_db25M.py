import os

import psycopg2
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from .config import get_dev_25M_db_params
import yaml


if __name__ == "__main__":
    with open("parameters.yaml", "r") as fp:
        params = yaml.safe_load(fp)
    num_movies_thresh = params["movie_thresh"]
    test_frac = params["test_frac"]

    spark = (
        SparkSession.builder.appName("movielens-25M")
        .config("spark.memory.offHeap.enabled", True)
        .config("spark.memory.offHeap.size", "16g")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )

    ratings_csv_file = os.path.join("data", "25M", "ratings.csv")

    df: DataFrame = spark.read.csv(ratings_csv_file, header=True, inferSchema=True)
    df.createOrReplaceTempView("ratings")

    df_movies_grouped = spark.sql(
        """
        SELECT movieId, COUNT(*) num_ratings
        FROM ratings
        GROUP BY movieId
        ORDER BY num_ratings
    """
    )

    df_movies_grouped_many_ratings = spark.sql(
        f"""
        SELECT movieId, COUNT(*) num_ratings, AVG(rating) avg_rating
        FROM ratings
        GROUP BY movieId
        HAVING num_ratings >= {num_movies_thresh}
        ORDER BY num_ratings
        """
    )
    df_movies_grouped_many_ratings.createOrReplaceTempView("movie_many_ratings")

    df_rating_reduced: DataFrame = spark.sql(
        """SELECT r.*, m.num_ratings, m.avg_rating
        FROM ratings r
        INNER JOIN movie_many_ratings m
        ON r.movieId=m.movieId"""
    )

    df_rating_reduced.createOrReplaceTempView("ratings_data_reduced")

    ratings = [
        row["rating"] for row in df_rating_reduced.select("rating").distinct().collect()
    ]

    eps = 0.01
    train: DataFrame = None
    test: DataFrame = None

    for rating in ratings:
        df_this_rating = spark.sql(
            f"SELECT * FROM ratings_data_reduced WHERE rating>={rating-eps} AND rating <= {rating+eps}"
        )
        this_rating_train, this_rating_test = df_this_rating.randomSplit(
            [1.0 - test_frac, test_frac], seed=42
        )
        if train is None:
            train = this_rating_train
            test = this_rating_test
        else:
            train = train.union(this_rating_train)
            test = test.union(this_rating_test)

    num_train = train.count()
    num_test = test.count()
    total_num = df_rating_reduced.count()

    print(f"{num_train/total_num*100:.2f} percent of ratings used for training")
    print(f"{num_test/total_num*100:.2f} percent of ratings used for testing")

    db_params = get_dev_25M_db_params("config.yaml", "config_secret.yaml")
    print(db_params)

    host = db_params["host"]
    database = db_params["database"]
    user = db_params["user"]
    password = db_params["password"]
    port = db_params["port"]

    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
    )

    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS ratings")
    cur.execute("DROP TABLE IF EXISTS ratings_train")
    cur.execute("DROP TABLE IF EXISTS ratings_test")

    df_rating_reduced.write.format("jdbc").option(
        "url", f"jdbc:postgresql://{host}:{port}/{database}"
    ).option("driver", "org.postgresql.Driver").option("dbtable", "ratings").option(
        "user", f"{user}"
    ).option(
        "password", f"{password}"
    ).save()

    test.write.format("jdbc").option(
        "url", f"jdbc:postgresql://{host}:{port}/{database}"
    ).option("driver", "org.postgresql.Driver").option(
        "dbtable", "ratings_test"
    ).option(
        "user", f"{user}"
    ).option(
        "password", f"{password}"
    ).save()

    train.write.format("jdbc").option(
        "url", f"jdbc:postgresql://{host}:{port}/{database}"
    ).option("driver", "org.postgresql.Driver").option(
        "dbtable", "ratings_train"
    ).option(
        "user", f"{user}"
    ).option(
        "password", f"{password}"
    ).save()
