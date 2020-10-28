import os.path as osp

import pytest
import pandas as pd
from sqlalchemy import create_engine

from pagai.services.database_explorer import get_sql_url, table_exists
from tests.settings import DATABASES


def get_test_data_path(filename: str) -> str:
    this_directory = osp.dirname(osp.realpath(__file__))
    return osp.join(this_directory, "data", filename)


@pytest.fixture(scope="session", params=list(DATABASES.keys()))
def db_config(request):
    db_driver = request.param
    db_config = DATABASES[db_driver]

    sql_engine = create_engine(get_sql_url(db_driver, db_config))

    # Load (or reload) test data into db.
    load_table(sql_engine, "patients", "patients.csv")
    load_table(sql_engine, "UPPERCASE", "patients-uppercase.csv")
    load_table(sql_engine, "CaseSensitive", "patients-case-sensitive.csv")

    db_config["model"] = db_driver
    return db_config


def load_table(sql_engine, table_name, data_file):
    data = pd.read_csv(
        get_test_data_path(data_file), sep=",", encoding="utf-8", parse_dates=["date"]
    )
    # Use custom check if table exists instead of pandas feature
    # df.to_sql(if_exists='replace') because it uses reflection on Oracle and
    # it's very slow.
    exists, table = table_exists(sql_engine, table_name)
    if exists:
        table.drop(sql_engine)
        print("dropped existing test table:", table_name)

    data.to_sql(name=table_name, con=sql_engine)
