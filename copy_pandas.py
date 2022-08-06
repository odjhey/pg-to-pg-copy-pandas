import os
import pandas as pd
from functools import reduce
from sqlalchemy import create_engine, dialects
from dotenv import load_dotenv


def main():

    load_dotenv()
    SRC_DB_CONN = os.getenv("SRC_DB_CONN")
    TARGET_DB_CONN = os.getenv("TARGET_DB_CONN")

    LIMIT = os.getenv("LIMIT") or 10
    CHUNK = os.getenv("CHUNK") or 100_000
    print("limit: ", LIMIT)
    print("chunk: ", CHUNK)

    src_conn = create_engine(
        SRC_DB_CONN,
        echo=True,
    )

    target_conn = create_engine(
        TARGET_DB_CONN,
        echo=True,
    )

    for table_name in src_conn.engine.table_names():
        print(table_name)

        for df in pd.read_sql(
            f'SELECT * FROM "{table_name}" LIMIT {LIMIT}',
            con=src_conn,
            chunksize=1000,
        ):
            new_dtype = get_new_dtypes(table_name, src_conn)

            df.to_sql(
                con=target_conn,
                name=table_name,
                index=False,
                if_exists="replace",
                dtype=new_dtype,
            )


def get_new_dtypes(table_name, db_conn):
    """
    dict/jsonb is not supported directly by df.to_sql
    so have to manually set the columns dtypes, jsonb => dialects.postgresql.JSONB
    """
    for df in pd.read_sql(
        f"SELECT * FROM information_schema.columns WHERE table_name = '{table_name}'",
        con=db_conn,
        chunksize=1000,
    ):

        cols_with_types = df[["column_name", "data_type"]].query("data_type == 'jsonb'")
        cols_with_types.data_type.replace(
            "jsonb", dialects.postgresql.JSONB, inplace=True
        )

        dtype_new = map(
            lambda x: {x["column_name"]: x["data_type"]},
            cols_with_types.to_dict(orient="records"),
        )

        def merge_dicts(x, y):
            z = x.copy()
            z.update(y)
            return z

        new_dtype = reduce(merge_dicts, list(dtype_new), {})
        return new_dtype


if __name__ == "__main__":
    main()
