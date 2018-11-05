import random
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
    if isinstance(column_names, str):
        column_names = [column_names]

    columns = {}
    for column_name in column_names:
        table, column = column_name.split('.')
        order_limit = 'ORDER BY '

        # If there is a weight for sampling, use log log to have frequent but various rows
        if has_frequency(table):
            order_limit += 'LOG(LOG(frequency+2)) * RANDOM() DESC '
        else:
            order_limit += 'RANDOM() '

        # Add limit if given
        if isinstance(limit, int):
            order_limit += 'LIMIT {}'.format(limit)

        # Assemble and run SQL query
        query = 'SELECT {} FROM {} {};'.format(column, table, order_limit)
        result = run(query)

        # Post-process: unwrap from rows and re-order
        rows = [res[0] for res in result]
        random.shuffle(rows)
        columns[column] = rows
    return columns