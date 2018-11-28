import datetime
import random

import numpy as np
import psycopg2

from engine.config import Config


def cache(request):
    """
    Caching decorator for storage, to cache all redundant sql queries
    """
    storage = {}

    def store(*args, connection=None, **kwargs):
        # Remove connection arg if given as args
        sql_args = []
        for arg in args:
            if isinstance(arg, psycopg2.extensions.connection):
                connection = arg
            else:
                sql_args.append(arg)

        # Build identifier based on args and kwargs
        identifier = (
            ";".join(map(str, sql_args))
            + "#"
            + ";".join(["{}:{}".format(k, v) for k, v in kwargs.items()])
        )

        # Search storage based on identifier
        if identifier not in storage:
            result = request(*sql_args, connection=connection, **kwargs)
            storage[identifier] = result

        return storage[identifier]

    return store


def get_sql_config(database="training_database"):
    config = Config("sql")
    if not hasattr(config, database):
        raise AttributeError(
            "The database {} is not configured for the engine.".format(database)
        )
    return getattr(config, database).to_dict()


def run(queries, connection=None):
    """
    Execute queries and can create a sql connection if needed
    """
    if connection is None:
        sql_params = get_sql_config("training_database")
        with psycopg2.connect(**sql_params) as connection:
            results = execute(queries, connection)

            connection.commit()
    else:
        results = execute(queries, connection)

    return results if isinstance(queries, list) > 1 else results[0]


def execute(queries, connection):
    """
    Execute queries with a given sql connection
    """
    results = []
    with connection.cursor() as cursor:
        if not isinstance(queries, list):
            queries = [queries]
        for query in queries:
            cursor.execute(query)
            result = cursor.fetchall()
            results.append(result)
    return results


@cache
def get_length(table, connection=None):
    """
    Return the length of a table
    """
    query = "SELECT count(*) FROM {};".format(table)
    length = run(query, connection)
    return length[0][0]


@cache
def get_tables(connection=None):
    """
    Return all table names in the active db
    """
    query = (
        "select table_name "
        "from information_schema.tables "
        "where table_schema = 'public';"
    )

    result = run(query, connection)
    tables = np.array(result).T[0]
    return tables


@cache
def get_table(table, connection=None, limit=1000):
    """
    Return content of a table with a limit
    """
    query = "SELECT * FROM {} ORDER BY RANDOM() LIMIT {};".format(table, limit)
    results = run(query, connection)
    results = np.array(results)
    return results


@cache
def get_columns(table, connection=None, include_data_type=False):
    """
    Return column names of a table
    """
    info = ["column_name"]
    if include_data_type:
        info.append("data_type")
    query = (
        "SELECT {} "
        "FROM information_schema.columns "
        "WHERE table_name='{}';".format(", ".join(info), table)
    )
    result = run(query, connection)
    if include_data_type:
        columns = result
    else:
        columns = np.array(result).T[0]
    return columns


@cache
def get_column(table, column, connection=None):
    """
    Return one column's table content with limit
    Used only to find value in the content, so the result is converted in a set.
    """
    table_len = get_length(table, connection)
    limit = max(5000, round(table_len ** (2 / 3)))
    query = (
        "SELECT {} "
        "FROM {} "
        "ORDER BY RANDOM() LIMIT {}".format(column, table, limit)
    )
    results = run(query, connection)
    results = set([res[0] for res in results])

    return results


def has_frequency(table, connection=None):
    """
    Check if there is a column frequency, which will act as a weight for sampling
    """
    query = (
        "SELECT column_name "
        "FROM information_schema.columns "
        "WHERE table_name='{}' and column_name='{}';".format(table, "frequency")
    )
    result = run(query, connection)
    return len(result) > 0


def fetch_columns(column_names, dataset_size, load_bar=None):
    """
    Given a spec in column_names, and a dataset_size,
    return extracted columns that will be used for training
    """
    sql_params = get_sql_config("training_database")
    with psycopg2.connect(**sql_params) as connection:
        # Put arg in a list if it is not the case
        if isinstance(column_names, (str, tuple)):
            column_names = [column_names]

        i_col = 0
        columns = []
        for column_name in column_names:
            column_name, nb_datasets = column_name

            table, column = column_name.split(".")

            n_rows = get_length(table, connection)

            # If there is a weight for sampling, use log log to have frequent but various rows
            weighted_sampling = has_frequency(table, connection)
            order_limit = "ORDER BY "
            if weighted_sampling:
                order_limit += "LOG(LOG(frequency+2)) * RANDOM() DESC "
            else:
                order_limit += "RANDOM() "

            # Add limit (note that we multiply with nb_datasets)
            order_limit += "LIMIT {}".format(dataset_size * nb_datasets)

            # Assemble SQL query
            query = "SELECT {} FROM {} {};".format(column, table, order_limit)

            # Run SQL to get samples of the database
            sampled_rows = run(query, connection)

            if len(sampled_rows) == 0:
                continue

            # Post-process: unwrap from rows, randomly re-order
            sampled_rows = [row[0] for row in sampled_rows]
            random.shuffle(sampled_rows)

            # Post-process: convert to str if needed
            for i, row in enumerate(sampled_rows):
                if row is None:
                    sampled_rows[i] = ""
                elif isinstance(row, str):
                    pass
                elif isinstance(row, (int, float)):
                    sampled_rows[i] = str(row)
                elif isinstance(row, datetime.date):
                    sampled_rows[i] = row.isoformat()
                else:
                    raise TypeError("Format of row is not supported", type(row), row)

            # Choose table rows ids with a uniform sampling with replacement strategy
            datasets_virtual_ids = np.random.randint(
                0, n_rows, (nb_datasets, dataset_size)
            )

            # List all unique ids selected
            virtual_unique_ids = list(
                set(datasets_virtual_ids.reshape(1, -1)[0].tolist())
            )

            # Map ids to real row indices of sampled_rows
            virtual_sampled_mapping = {
                v_id: s_id for s_id, v_id in enumerate(virtual_unique_ids)
            }
            mapper = lambda x: virtual_sampled_mapping[x]

            for i in range(nb_datasets):
                # Find the ids for the given dataset corresponding to the sampled_rows
                virtual_ids = datasets_virtual_ids[i]
                sampled_ids = np.vectorize(mapper)(virtual_ids)

                # Get the rows
                rows = [sampled_rows[row_id] for row_id in sampled_ids]

                columns.append((column_name, rows))

                # Update the progress bar
                if load_bar is not None:
                    i_col += 1
                    load_bar.value = i_col
        return columns
