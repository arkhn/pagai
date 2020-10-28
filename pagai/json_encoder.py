import datetime
import decimal
import flask


class MyJSONEncoder(flask.json.JSONEncoder):
    """
    We make our JSONEncoder to override the default method.
    When jsonifying the rows retrieved by SQLAlchemy, we need to handle type
    serialization manually when flask does not handle them already.
    - SQLALchemy parses DB "number" objects to decimal.Decimal so we convert
        them to floats
    - SQLAlchemy parses DB "date" objects to datetime.date so we convert
        them to strings
    """

    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            # Convert decimal instances to floats
            return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return super(MyJSONEncoder, self).default(obj)
