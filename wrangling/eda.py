"""
NFL EDA of individual files using pyspark
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import utils
import sys

# initialise spark session
sc = SparkSession.builder.appName('nfl-eda')\
    .master("local[15]").getOrCreate()

# read in games
games_df = sc.read.csv("../data/games.csv",
                       header=True,
                       inferSchema=True)

utils.get_raw_data_info(games_df)

# sort by date and time
games_df = games_df.sort("gameDate", "gameTimeEastern")

# rename some of the columns that end in "Abbr"
games_df = games_df.toDF(*(c.replace("Abbr", "") for c in games_df.columns))
print(games_df.columns)


