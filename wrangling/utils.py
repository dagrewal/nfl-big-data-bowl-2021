
from pyspark.sql.functions import *
from pyspark.sql.types import *

def drop_duplicate_columns(dataset):
    """
    Removes duplicate columns from a dataset after a merge between two datasets.

    Args:
        dataset: dataset containing duplicate columns

    Returns:
        output_df: dataset containing de-duplicated columns
    """

    # create empty list to hold duplicate column names
    cols_new = []

    # create empty set to hold column names to keep
    seen = set()

    # iterate through dataset column names and append to list and set depending whether
    # or not column name is duplicate to remove or to keep
    for c in dataset.columns:
        cols_new.append("{}_dup".format(c) if c in seen else c)
        seen.add(c)

    # create output dataset by removing duplicate columns
    output_df = dataset.toDF(*cols_new).select(*[c for c in cols_new
                                                 if not c.endswith("_dup")])

    return output_df


def get_raw_data_info(df):
    """
    Get basic information on data after reading it in.

    Args:
        df: Spark Dataset
    
    Returns:
        None
    """
    if (df != None) or (len(df) != 0):
        # get first ten rows
        df.show(10, truncate=True)

        # count dimensions
        print("Number of rows: {}\n".format(df.count()))
        print("Number of columns: {}\n".format(len(df.columns)))
        
        # get schema
        print("Printing dataset schema...\n")
        df.printSchema()

        # Count unique values in each column
        print("# of Distinct Values in Each Column:\n")
        for col in df.columns:
            distinct_n = df.select(col).distinct().count()
            print(f"{col}: {distinct_n}")

        # Count missing values in each column
        print("Printing counts of missing values...\n")
        df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()

        # identify duplicate rows
        print("Identifying duplicate rows...\n")
        duplicate_rows = df.groupBy(df.columns).count().filter(col("count") > 1).collect()
        if len(duplicate_rows) > 0:
            print("Duplicated rows have been identified:\n")
            print(duplicate_rows)
        print("There were no duplicate rows in the dataset.\n")
    else:
        print("Dataset appears to be empty.")