import pandas


def transform_df(df, config):
    trans_list = config.get("Transformation", [])
    for trans_elm in trans_list:
        trans_func_name = trans_elm["FunctionName"]
        trans_func = globals()[trans_func_name]
        args = trans_elm.get("Arguments", {})
        trans_type = trans_elm.get("Type", "Series")
        if trans_type == "DataFrame":
            df = trans_func(df, **args)
        else:
            columns = trans_elm.get("Columns", df.columns)
            for column in columns:
                df[column] = trans_func(df[column], **args.get(column, {}))
    return df


def cast_col(col, dtype):
    return col.astype(dtype)


def to_numeric(col):
    from decimal import Decimal

    return col.map(lambda x: x if pandas.isna(x) else Decimal(x))


def to_datetime(col, utc=False):
    return pandas.to_datetime(col, utc=utc)


def to_date(col):
    return col.map(lambda dt: dt.date() if not pandas.isna(dt) else dt)


def to_time(col):
    return col.map(lambda dt: dt.time() if not pandas.isna(dt) else dt)


def to_timetz(col):
    return col.map(lambda dt: dt.timetz() if not pandas.isna(dt) else dt)


def drop_index(df):
    return df.reset_index(drop=True)
