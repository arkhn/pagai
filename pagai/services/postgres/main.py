import datetime
import numpy as np
import os
import psycopg2
import random

from pagai.errors import OperationOutcome


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


def get_sql_config():
    return {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }


def run(queries, connection=None):
    """
    Execute queries and can create a sql connection if needed
    """
    if connection is None:
        sql_params = get_sql_config()
        with psycopg2.connect(**sql_params) as connection:
            results = execute(queries, connection)

            connection.commit()
    else:
        results = execute(queries, connection)

    # TODO: return actual results rather than parsed results
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


def from_pyrog_credentials(credentials: dict):
    return psycopg2.connect(
        **{
            "host": credentials.get("host"),
            "port": credentials.get("port"),
            "dbname": credentials.get("database"),
            "user": credentials.get("login"),
            "password": credentials.get("password"),
        }
    )


@cache
def get_length(table, connection=None):
    """
    Return the length of a table
    """
    query = "SELECT count(*) FROM {};".format(table)
    length = run(query, connection)
    return length[0][0]


@cache
def get_table_names(connection=None):
    """
    Return all table names in the active database, on conditions:
    * remove table names which belong to the same partition
    * only fetch table names belonging to relevant postgres database schema
    """

    query = f"SELECT table_name FROM information_schema.tables;"

    # If PG_DB_SCHEMA is not "", then use it.
    # It prevents fetching tables which belong to pg_catalog or information_schema
    if os.getenv("PG_DB_SCHEMA"):
        query = f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema='{os.getenv('PG_DB_SCHEMA')}';"
    all_table_names = run(query, connection)

    # Filter out all table names which belong to a partition
    # and keep only partition name
    partition_query = """
WITH partitions AS (SELECT
    string_agg(child.relname, ', ') AS tables,
    inhparent
FROM pg_inherits
    JOIN pg_class child ON pg_inherits.inhrelid = child.oid
GROUP BY inhparent)
SELECT tables, parent.relname
FROM partitions
JOIN pg_class parent ON inhparent = parent.oid;"""
    partitions = run(partition_query, connection)

    for partition in partitions:
        partition_table_names = partition[0].split(", ")
        all_table_names = [name for name in all_table_names if name[0] not in partition_table_names]
    tables = np.array(all_table_names).T[0]

    return tables


@cache
def explore(table, connection, limit, schema=None):
    """
    Returns the first rows of a table alongside the column names.
    """
    try:
        return {
            "fields": get_column_names(table, schema=schema, connection=connection).tolist(),
            "rows": get_table(table, schema=schema, connection=connection, limit=limit).tolist(),
        }
    except psycopg2.OperationalError as e:
        raise OperationOutcome(e)
    except psycopg2.errors.UndefinedTable:
        raise OperationOutcome(f"Table {table} does not exist in database")


@cache
def get_table(table, connection=None, limit=1000, schema=None):
    """
    Return content of a table with a limit
    """
    if schema:
        table = f'"{schema}"."{table}"'

    query = f"SELECT * FROM {table} ORDER BY RANDOM() LIMIT {limit};"
    results = run(query, connection)
    results = np.array(results)
    return results


@cache
def get_column_names(table, connection=None, include_data_type=False, schema=None):
    """
    Return column names of a table
    """
    info = ["column_name"]
    if include_data_type:
        info.append("data_type")
    query = (
        f"SELECT {', '.join(info)} "
        "FROM information_schema.columns "
        f"WHERE table_name='{table}'"
    )
    if schema:
        query = f"{query} AND table_schema='{schema}'"

    result = run(query, connection)
    if include_data_type:
        columns = result
    else:
        columns = np.array(result).T[0] if len(result) > 0 else np.array(result)
    return columns


@cache
def get_column(table, column, limit=None, order=None, connection=None):
    """
    Return one column's table content with limit
    Used only to find value in the content, so the result is converted in a set.
    """
    table_len = get_length(table, connection)
    limit = limit or max(5000, round(table_len ** (2 / 3)))
    order = order or "RANDOM()"
    query = f"SELECT {column} FROM {table} ORDER BY {order} LIMIT {limit}"
    results = run(query, connection)
    results = set([res[0] for res in results])

    return results


storage = {}


def get_column_fast(table, column, limit=None, order=None, connection=None):
    # Build identifier based on args and kwargs
    identifier = f"{table}:{limit}:{order}"

    # Search storage based on identifier
    if identifier not in storage:
        print("compute")
        table_len = get_length(table, connection)
        limit = limit or max(5000, round(table_len ** (2 / 3)))
        order = order or "RANDOM()"
        query = f"SELECT * FROM {table} ORDER BY {order} LIMIT {limit}"
        results = run(query, connection)

        columns = get_column_names(table, connection)

        results = np.array(results)

        storage[identifier] = {}
        for idx, col in enumerate(columns):
            storage[identifier][col] = results[:, idx]
    else:
        print("cached")

    return storage[identifier][column]


def fetch_columns(datasets, dataset_size, connection=None):
    """
    Given a spec in column_names, and a dataset_size,
    return extracted columns that will be used for training and classification
    """
    # Put arg in a list if it is not the case
    if isinstance(datasets, (str, tuple)):
        datasets = [datasets]

    columns = []
    print(len(datasets), "datasets")
    for i, dataset in enumerate(datasets):
        print(i, "/", len(datasets))

        table_column_name, nb_datasets = dataset
        table, column = table_column_name.split(".")

        n_rows = get_length(table, connection)

        # If there is a weight for sampling, use log log to have frequent but various rows
        weighted_sampling = has_frequency(table, connection)
        if weighted_sampling:
            order = "LOG(LOG(frequency+2)) * RANDOM() DESC"
        else:
            order = "RANDOM()"

        # Add limit (note that we multiply by nb_datasets)
        limit = dataset_size * nb_datasets

        # Call the cached method to get samples of the database
        print(table, ":", column, "?", end="\t")
        sampled_rows = get_column_fast(table, column, limit, order, connection)

        if len(sampled_rows) == 0:
            continue

        # Post-process: randomly re-order
        sampled_rows = sampled_rows.tolist()
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
                raise TypeError("Format of row is not supported", type(row), row, sampled_rows)

        # Choose table rows ids with a uniform sampling with replacement strategy
        datasets_virtual_ids = np.random.randint(0, n_rows, (nb_datasets, dataset_size))

        # List all unique ids selected
        virtual_unique_ids = list(set(datasets_virtual_ids.reshape(1, -1)[0].tolist()))

        # Map ids to real row indices of sampled_rows
        virtual_sampled_mapping = {v_id: s_id for s_id, v_id in enumerate(virtual_unique_ids)}

        def mapper(x):
            return virtual_sampled_mapping[x]

        for i in range(nb_datasets):
            # Find the ids for the given dataset corresponding to the sampled_rows
            virtual_ids = datasets_virtual_ids[i]
            sampled_ids = np.vectorize(mapper)(virtual_ids)

            # Get the rows
            rows = [sampled_rows[row_id] for row_id in sampled_ids]

            columns.append((column, rows))

    return columns


def has_frequency(table, connection=None):
    """
    Check if a table has a column frequency, which will act as a weight for sampling
    """
    query = (
        f"SELECT column_name "
        f"FROM information_schema.columns "
        f"WHERE table_name='{table}' and column_name='frequency';"
    )
    result = run(query, connection)
    return len(result) > 0
