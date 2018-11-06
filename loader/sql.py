import random
import datetime
import psycopg2
import logging

from loader import Credential

def run(query):
    with psycopg2.connect(host="localhost",
                          port=5432,
                          database=Credential.DATABASE.value,
                          user=Credential.USER.value,
                          password=Credential.PASSWORD.value) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            output_row = cursor.fetchall()

        connection.commit()

    return output_row


def has_frequency(table):
    """
    Check if there is a column frequency, which will act as a weight for sampling
    """
    query = "SELECT column_name " \
            "FROM information_schema.columns " \
            "WHERE table_name='{}' and column_name='{}';".format(table, 'frequency')
    result = run(query)
    return len(result) > 0


def fetch_columns(column_names, limit=None):
    # Put arg in a list if it is not the case
    if isinstance(column_names, (str, tuple)):
        column_names = [column_names]

    columns = {}
    for column_name in column_names:
        column_name, nb_datasets = column_name

        table, column = column_name.split('.')

        # If there is a weight for sampling, use log log to have frequent but various rows
        order_limit = 'ORDER BY '
        if has_frequency(table):
            order_limit += 'LOG(LOG(frequency+2)) * RANDOM() DESC '
        else:
            order_limit += 'RANDOM() '

        # Add limit if given (note that we multiply with nb_datasets: we avoid duplicates)
        if isinstance(limit, int):
            order_limit += 'LIMIT {}'.format(limit)

        # Assemble SQL query
        query = 'SELECT {} FROM {} {};'.format(column, table, order_limit)

        # Run query and build the datasets
        datasets = []
        for i in range(nb_datasets):
            # Run SQL
            result = run(query)

            # Post-process: unwrap from rows, randomly re-order
            rows = [res[0] for res in result]
            random.shuffle(rows)

            if limit is not None and len(rows) < limit:
                logging.warning(
                    "Columns for {} couldn't be filled completely ({}/{})".format(
                        column_name, len(rows), limit)
                )

            # Post-process: convert to str if needed
            if len(rows) > 0 and not isinstance(rows[0], str):
                str_rows = []
                for row in rows:
                    if row is None:
                        str_row = ''
                    elif isinstance(row, (int, float, str)):
                        str_row = str(row)
                    elif isinstance(row, datetime.date):
                        str_row = row.isoformat()
                    else:
                        raise TypeError('Format of input is not supported', row, )
                    str_rows.append(str_row)
                rows = str_rows
            datasets.append(rows)

        columns[column_name] = datasets
    return columns