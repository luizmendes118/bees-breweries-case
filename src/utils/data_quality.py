from pyspark.sql import DataFrame


def validate_breweries(df: DataFrame):

    if df.limit(1).count() == 0:
        raise ValueError("Dataset is empty")

    if df.filter(df.country.isNull()).limit(1).count() > 0:
        raise ValueError("Null values found in country column")

    if df.filter(df.state.isNull()).limit(1).count() > 0:
        raise ValueError("Null values found in state column")

    return True
