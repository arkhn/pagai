import os.path as osp

import pytest
import pandas as pd
from sqlalchemy import create_engine

from pagai.services.database_explorer import get_sql_url, table_exists
from test.settings import DATABASES


def get_test_data_path(filename: str) -> str:
    this_directory = osp.dirname(osp.realpath(__file__))
    return osp.join(this_directory, 'data', filename)


@pytest.fixture(scope='session',
                params=list(DATABASES.keys()))
def db_config(request):
    db_handler = request.param
    db_config = DATABASES[db_handler]

    # Load (or reload) test data into db.
    data = pd.read_csv(get_test_data_path('patients.csv'),
                       sep=',',
                       encoding='utf-8',
                       parse_dates=['date'])
    sql_engine = create_engine(get_sql_url(db_handler, db_config))

    table_name = 'patients'

    # Use custom check if table exists instead of pandas feature df.to_sql(if_exists='replace') because it uses
    # reflection on Oracle and it's very slow.
    if table_exists(sql_engine, table_name):
        sql_engine.execute('drop table %s' % table_name)
        print('dropped existing test table:', table_name)

    data.to_sql(name=table_name, con=sql_engine)

    db_config['handler'] = db_handler
    return db_config
