import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text, inspect

SQL_USERNAME = "root"
SQL_PASSWORD = "sarkar02"
SQL_HOST = "localhost"
SQL_PORT = "3306"
SQL_DATABASE = "invoicedetails"

connection_string = f"mysql+pymysql://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_HOST}:{SQL_PORT}/{SQL_DATABASE}"

# Create the engine
engine = create_engine(connection_string)
inspector = inspect(engine)


def get_table_columns(table_name):
    if not inspector.has_table(table_name):
        print(f"Table with name{table_name} does not exist")
        return []
    else:
        columns = [column["name"] for column in inspector.get_columns(table_name)]
        return columns


def upload_df_to_sql(df, table_name):
    if not df.empty:
        df = convert_df(df)
        if not inspector.has_table(table_name):
            print(f"Table '{table_name}' does not exist. Creating new table.")
            df.to_sql(f"{table_name}", con=engine, if_exists="append", index=False)

        else:
            query = text(f"SELECT * FROM {table_name}")
            existing_df = pd.read_sql(query, con=engine)
            print(f"EXISTING DF is \n {existing_df} \n")
            to_append_df = df[~df["Booking_ID"].isin(existing_df["Booking_ID"])]
            print(f"DATAFRAME TO BE APPENDED IS \n {to_append_df} \n")
            to_append_df.to_sql(
                f"{table_name}", con=engine, if_exists="append", index=False
            )


def Remove_NaN_in_df(dataframe):
    # Check if any value is NaN
    if dataframe.isna().any().any():
        print("DataFrame contains NaN values")
        dataframe = dataframe.replace([np.nan, -np.inf], 0)
        return dataframe
    else:
        print("DataFrame does not contain NaN values")
        return dataframe


def convert_df(df):
    for col in df.select_dtypes(include="object").columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except ValueError:
            try:
                df[col] = pd.to_datetime(df[col])
            except ValueError:
                continue

    df = df.convert_dtypes()
    return df
