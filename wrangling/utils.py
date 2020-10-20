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