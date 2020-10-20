"""
ETL NFL data
"""
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils import drop_duplicate_columns
import glob
import sys

# initialise spark session
sc = SparkSession.builder.appName('nfl-data-prep')\
    .config("spark.sql.shuffle.partitions", "50")\
        .config("spark.driver.maxResultSize", "5g")\
            .config("spark.sql.execution.arrow.enabled", "true")\
                .getOrCreate()

def etl_games_plays(**data_files):
    """
    Prepare the separate raw data files into one large dataset.

    Args:
        **data_files: dict (games='games', plays='plays')
    
    Return:
        games_plays_df: merged dataset of games and plays from ../data
    """

    # read in data from ../data and register as temptable
    for name, csv in data_files.items():
        name = sc.read.csv("../data/{}.csv".format(csv), header=True)
        name.registerTempTable("{}".format(csv))

    # merge games with plays
    games_plays = sc.sql("""
                         SELECT games.*, plays.*
                         FROM games
                         JOIN plays ON games.gameId = plays.gameId
                         """)

    

    # drop duplicate columns from merged dataset
    games_plays_df = drop_duplicate_columns(games_plays)

    return games_plays_df

def etl_weeks(data_files):
    """
    Concatenates all the raw week files from ../data

    Args:
        data_files: list containing file names, see main() for more details

    Return:
        weeks_df: concatenated dataset
    """

    # create schema
    schema = StructType([
        StructField("time", StringType(), True),
        StructField("x", StringType(), True),
        StructField("y", StringType(), True),
        StructField("s", StringType(), True),
        StructField("a", StringType(), True),
        StructField("dis", StringType(), True),
        StructField("o", StringType(), True),
        StructField("dir", StringType(), True),
        StructField("event", StringType(), True),
        StructField("nflId", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("jerseyNumber", StringType(), True),
        StructField("position", StringType(), True),
        StructField("frameId", StringType(), True),
        StructField("team", StringType(), True),
        StructField("gameId", StringType(), True),
        StructField("playId", StringType(), True),
        StructField("playDirection", StringType(), True),
        StructField("route", StringType(), True)
    ])

    # initialise empty dataset
    weeks_df = sc.createDataFrame([], schema=schema)

    # read in data from ../data and 
    for csv in data_files:
        name = sc.read.csv("../data/{}.csv".format(csv), header=True)
        weeks_df = weeks_df.unionAll(name)

    return weeks_df

def etl_players_weeks(**data_files):
    """
    Merge players and output from etl_weeks()

    Args:
        data_files: dict (players='players', conc_weeks='conc_weeks')

    Return:
        players_weeks_df: merged dataset
    """

    # read in data from ../data
    for name, csv in data_files.items():
        name = sc.read.csv("../data/{}.csv".format(csv), header=True)
        name.registerTempTable(csv)

    # merge tables together
    players_weeks = sc.sql("""
                         SELECT players.*, conc_weeks.*
                         FROM players
                         JOIN conc_weeks ON players.nflId = conc_weeks.nflId
                         """)

    # drop duplicate columns from merged dataset
    players_weeks_df = drop_duplicate_columns(players_weeks)

    return players_weeks_df

def etl_games_plays_players_weeks(**data_files):
    """
    Merge output from etl_players_weeks() and etl_games_plays() into final dataset

    Args:
        data_files: dict (games_plays='games_plays', players_weeks='players_weeks')

    Return:
        final_df: final merged dataset containing all data
    """

    # read in data from ../data
    for name, csv in data_files.items():
        name = sc.read.csv("../data/{}.csv".format(csv), header=True)
        name.registerTempTable(csv)

    # merge tables together
    final = sc.sql("""
                      SELECT games_plays.*, players_weeks.*
                      FROM games_plays
                      JOIN players_weeks ON games_plays.playId = players_weeks.playId
                      """)

    # drop duplicate columns from merged data
    final_df = drop_duplicate_columns(final)

    return final_df

def save_data(df, save_name:str):
    """
    Save spark dataframe to csv in ../data

    Args:
        df: spark dataframe to save
        save_name: name of file where dataframe will be saved

    Return:
        None
    """
    df.write.csv("../data/{}.csv".format(save_name), mode='overwrite', header=True)

def main():
    """
    Run ETL pipeline
    """

    if len(sys.argv) == 1:
        
        print("RUNNING ETL PIPELINE...")

        # fetch the 'weeks' files from ../data
        weeks_files = glob.glob("../data/" + "week*")

        # initialise dict to hold files
        weeks_data = []

        # iterate through files and fetch only names of files
        for file in weeks_files:
            temp = file[8:-4]
            weeks_data.append(temp)

        print("CONCATENATING WEEKS FILES INTO ONE DATASET...")
        weeks_df = etl_weeks(weeks_data)

        print("SAVING CONCATENATED WEEKS FILE...")
        save_data(weeks_df, "conc_weeks")

        print("MERGING GAMES AND PLAYS FILES...")
        games_plays_df = etl_games_plays(games='games', plays='plays')

        print("SAVING MERGED GAMES AND PLAYS DATASET...")
        save_data(games_plays_df, "games_plays")

        print("MERGING PLAYERS AND CONCATENATED WEEKS...")
        players_weeks_df = etl_players_weeks(players='players', conc_weeks='conc_weeks')

        print("SAVING MERGED PLAYERS AND CONCATENATED WEEKS...")
        save_data(players_weeks_df, "players_weeks")

        print("MERGING DATA INTO FINAL OUTPUT DATASET...")
        final_df = etl_games_plays_players_weeks(games_plays='games_plays', players_weeks='players_weeks')

        print("SAVING FINAL MERGED DATASET...")
        save_data(final_df, "all_data")
    else:
        print("Could not run ETL pipeline. Please make sure that you "\
            "run the command as follows: "\
                "python etl.py")
        
if __name__ == '__main__':
    main()
            

        