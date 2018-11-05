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


def fetch_columns(column_names, limit=None):
    if isinstance(column_names, str):
        column_names = [column_names]

    if isinstance(limit, int):
        limit = 'ORDER BY RANDOM() LIMIT {}'.format(limit)
    else:
        limit = ''

    columns = {}
    for column_name in column_names:
        table, column = column_name.split('.')
        query = 'SELECT {} FROM {} {};'.format(column, table, limit)
        result = run(query)
        columns[column] = [res[0] for res in result]
    return columns