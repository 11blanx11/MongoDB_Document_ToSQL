import numpy as np 
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from collections import defaultdict

SQL_USERNAME = "root"
SQL_PASSWORD = "sarkar02"
SQL_HOST = "localhost"
SQL_PORT = "3306"
SQL_DATABASE = "invoicedetails"

connection_string = f"mysql+pymysql://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_HOST}:{SQL_PORT}/{SQL_DATABASE}"

# Create the engine
engine = create_engine(connection_string)

inspector = inspect(engine)


def null_column_handling(lst):
    count = 0
    result = []
    for item in lst:
        if item is None or item == "":
            count += 1
            result.append(f"Unnamed{count}")
        else:
            result.append(item)
    return result


def get_table_columns(table_name):
    if not inspector.has_table(table_name):
        print(f"Table with name{table_name} does not exist")
        return []
    else:
        columns = [column["name"] for column in inspector.get_columns(table_name)]
        return columns


def column_rename(lst):
    # Dictionary to count occurrences of each column name
    column_count = defaultdict(int)

    # Loop through keyslist to process column names
    processed_columns = []
    for col in lst:
        # Strip leading/trailing spaces and replace internal spaces with underscores
        processed_col = col.strip().replace(" ", "_")

        # Check if column name has been encountered before
        if column_count[processed_col] > 0:
            # Append a number to the column name to make it unique
            processed_col = f"{processed_col}_{column_count[processed_col]}"

        # Increment count for this column name
        column_count[processed_col] += 1

        # Add processed column name to list
        processed_columns.append(processed_col)

    return processed_columns


def upload_df_to_sql(df, table_name):
    if not df.empty:
        df = convert_df(df)
        if not inspector.has_table(table_name):
            print(f"Table '{table_name}' does not exist. Creating new table.")
            df.to_sql(f"{table_name}", con=engine, if_exists="append", index=False)

        else:
            # ---------SQL QUERY----------------
            query = text(f"SELECT * FROM {table_name};")
            existing_df = pd.read_sql(query, con=engine)
            # --------Pandas Handling----------------
            # print(f"EXISTING DF is \n {existing_df} \n")
            to_append_df = df[~df["Booking_ID"].isin(existing_df["Booking_ID"])]
            # print(f"DATAFRAME TO BE APPENDED IS \n {to_append_df} \n")

            # Now comes the fun part - Concat the existing df to the one to be appended, drop the existing df and then send the final df to sql
            to_append_df = pd.concat([to_append_df, existing_df], ignore_index=True)
            to_append_df = handle_na_values(to_append_df)

            # ----------SQL Queries--------------------
            query = text(f"DROP TABLE {table_name};")
            with engine.connect() as connection:
                connection.execute(query)
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


def handle_na_values(df):
    # Define a dictionary with default fill values for each data type
    fill_values = {
        "float64": 0.0,
        "int64": 0,
        "object": "",  # for string columns
        "datetime64[ns]": pd.Timestamp("1970-01-01"),
        "bool": False,
    }

    # Iterate over each column and fill NaN values based on data type
    for col in df.columns:
        dtype = df[col].dtype
        if dtype in fill_values:
            df[col] = df[col].apply(lambda x: fill_values[dtype] if pd.isna(x) else x)
        # else:
        #     # Default to filling with None if data type is not in fill_values
        #     df[col].fillna(None, inplace=True)
    return df


def convert_df(df):
    df = df.convert_dtypes()
    for col in df.select_dtypes(include="object").columns:
        try:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        except ValueError:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except (ValueError, TypeError):
                continue

    df = df.convert_dtypes()
    return df
