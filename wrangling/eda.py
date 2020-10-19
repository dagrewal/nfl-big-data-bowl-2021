"""
exploring nfl data
"""
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

# initialise spark session
sc = SparkSession.builder.appName('nfl-eda')\
    .config("spark.sql.shuffle.partitions", "50")\
        .config("spark.driver.maxResultSize", "5g")\
            .config("spark.sql.execution.arrow.enabled", "true")\
                .getOrCreate()

# read games, players and week 1 and join into one table
games_df = sc.read.csv("../data/games.csv", header=True)
players_df = sc.read.csv("../data/players.csv", header=True)
plays_df = sc.read.csv("../data/plays.csv", header=True)
week1_df = sc.read.csv("../data/week1.csv", header=True)

games_df.dtypes
players_df.dtypes
plays_df.dtypes
week1_df.dtypes

games_df.registerTempTable("games")
plays_df.registerTempTable("plays")
week1_df.registerTempTable("week1")

games_plays = sc.sql("""select games.*, plays.*
                        from games 
                        join plays on games.gameId = plays.gameId""")
# drop duplicate cols
cols_new = []
seen = set()
for c in games_plays.columns:
    cols_new.append("{}_dup".format(c) if c in seen else c)
    seen.add(c)

games_plays_df = games_plays.toDF(*cols_new).select(*[c for c in cols_new if not c.endswith("_dup")])

games_plays_wk1_df = games_plays_df.join(week1_df, "playId", "inner")
games_plays_wk1_df.show(5)
games_plays_wk1_df.dtypes

# drop duplicate cols
cols_new = []
seen = set()
for c in games_plays_wk1_df.columns:
    cols_new.append("{}_dup".format(c) if c in seen else c)
    seen.add(c)

games_plays_wk1_df = games_plays_wk1_df.toDF(*cols_new).select(*[c for c in cols_new if not c.endswith("_dup")])