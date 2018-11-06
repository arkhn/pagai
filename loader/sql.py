import random
import datetime
import psycopg2

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

        nb_datasets = 1
        if isinstance(column_name, tuple):
            if len(column_name) >= 2:
                column_name, nb_datasets = column_name
            else:
                column_name = column_name[0]

        table, column = column_name.split('.')
        order_limit = 'ORDER BY '

        # If there is a weight for sampling, use log log to have frequent but various rows
        if has_frequency(table):
            order_limit += 'LOG(LOG(frequency+2)) * RANDOM() DESC '
        else:
            order_limit += 'RANDOM() '

        # Add limit if given (note that we multiply with nb_datasets: we avoid duplicates)
        if isinstance(limit, int):
            order_limit += 'LIMIT {}'.format(limit * nb_datasets)

        # Assemble and run SQL query
        query = 'SELECT {} FROM {} {};'.format(column, table, order_limit)
        result = run(query)

        # Post-process: unwrap from rows, randomly re-order
        rows = [res[0] for res in result]
        random.shuffle(rows)

        # Post-process: convert to str
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

        # Reorganize results in the right number of datasets
        datasets = []
        for i in range(nb_datasets):
            if limit is not None:
                datasets.append(rows[i*limit:(i+1)*limit])
                # if there's a limit but it's too high for length of rows,
                # just return all the rows and exit the loop
                if (i + 1) * limit >= len(rows):
                    break
            else:
                datasets.append(rows)
        columns[column_name] = datasets
    return columns