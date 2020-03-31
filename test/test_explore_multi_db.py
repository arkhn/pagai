import datetime

from pagai.services.database_explorer import DatabaseExplorer, POSTGRES


class TestExploration:
    def test_explore(self, db_config):
        explorer = DatabaseExplorer(db_config['handler'], db_config)
        exploration = explorer.explore(table='patients', limit=2)

        assert exploration['fields'] == ['index', 'patient_id', 'gender', 'date']
        assert sorted(exploration['rows']) == [[0, 1, 'F', datetime.datetime(1974, 3, 5, 0, 0)],
                                               [1, 2, 'M', datetime.datetime(1969, 12, 21, 0, 0)]]
