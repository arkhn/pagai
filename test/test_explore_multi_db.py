from collections import Counter
from datetime import datetime
from unittest import TestCase

from pagai.services.database_explorer import DatabaseExplorer, POSTGRES, MSSQL, ORACLE


OWNER_FOR_DBTYPE = {
    POSTGRES: "public",
    MSSQL: "dbo",
    ORACLE: "SYSTEM",
}


class TestExploration:
    def test_explore(self, db_config):
        explorer = DatabaseExplorer(db_config)

        if db_config["model"] == ORACLE:
            exploration = explorer.explore(
                table_name="PATIENTS", schema=OWNER_FOR_DBTYPE[db_config["model"]], limit=2,
            )
            TestCase().assertCountEqual(
                exploration["fields"], ["index", "PATIENT_ID", "GENDER", "date"]
            )
        else:
            exploration = explorer.explore(
                table_name="patients", schema=OWNER_FOR_DBTYPE[db_config["model"]], limit=2,
            )
            TestCase().assertCountEqual(
                exploration["fields"], ["index", "patient_id", "gender", "date"]
            )

        expected_rows = (
            ["F", datetime(1974, 3, 5, 0, 0), 0, 1],
            ["M", datetime(1969, 12, 21, 0, 0), 1, 2],
        )
        # Check that both expected rows are present in the result
        for expected_row in expected_rows:
            assert any(
                Counter(expected_row) == Counter(actual_row) for actual_row in exploration["rows"]
            )

    def test_owners(self, db_config):
        explorer = DatabaseExplorer(db_config)
        owners = explorer.get_owners()
        if db_config["model"] == POSTGRES:
            TestCase().assertCountEqual(
                owners,
                [
                    "pg_toast",
                    "pg_temp_1",
                    "pg_toast_temp_1",
                    "pg_catalog",
                    "public",
                    "information_schema",
                ],
            )
        elif db_config["model"] == MSSQL:
            TestCase().assertCountEqual(
                owners,
                [
                    "dbo",
                    "guest",
                    "INFORMATION_SCHEMA",
                    "sys",
                    "db_owner",
                    "db_accessadmin",
                    "db_securityadmin",
                    "db_ddladmin",
                    "db_backupoperator",
                    "db_datareader",
                    "db_datawriter",
                    "db_denydatareader",
                    "db_denydatawriter",
                ],
            )
        else:
            TestCase().assertCountEqual(
                owners,
                [
                    "SYS",
                    "AUDSYS",
                    "SYSTEM",
                    "SYSBACKUP",
                    "SYSDG",
                    "SYSKM",
                    "SYSRAC",
                    "OUTLN",
                    "XS$NULL",
                    "GSMADMIN_INTERNAL",
                    "GSMUSER",
                    "GSMROOTUSER",
                    "DIP",
                    "REMOTE_SCHEDULER_AGENT",
                    "DBSFWUSER",
                    "ORACLE_OCM",
                    "SYS$UMF",
                    "DBSNMP",
                    "APPQOSSYS",
                    "GSMCATUSER",
                    "GGSYS",
                    "XDB",
                    "ANONYMOUS",
                    "WMSYS",
                    "MDDATA",
                    "OJVMSYS",
                    "CTXSYS",
                    "ORDSYS",
                    "ORDDATA",
                    "ORDPLUGINS",
                    "SI_INFORMTN_SCHEMA",
                    "MDSYS",
                    "OLAPSYS",
                    "DVSYS",
                    "LBACSYS",
                    "DVF",
                ],
            )

    def test_get_db_schema(self, db_config):
        explorer = DatabaseExplorer(db_config)
        db_schema = explorer.get_db_schema(owner=OWNER_FOR_DBTYPE[db_config["model"]])
        self.verify_schema_structure(db_schema)

    def test_case_sensitivity(self, db_config):
        explorer = DatabaseExplorer(db_config)
        db_schema = explorer.get_db_schema(owner=OWNER_FOR_DBTYPE[db_config["model"]])

        all_tables = list(db_schema.keys())

        if db_config["model"] == ORACLE:
            # In the case of ORACLE, the table name "patients" was turned into "PATIENTS"
            test_tables = ["PATIENTS", "UPPERCASE", "CaseSensitive"]
        else:
            test_tables = ["patients", "UPPERCASE", "CaseSensitive"]

        for table in test_tables:
            assert table in all_tables

        for table in test_tables:
            exploration = explorer.explore(
                table_name=table, schema=OWNER_FOR_DBTYPE[db_config["model"]], limit=2,
            )

            TestCase().assertCountEqual(exploration["fields"], db_schema[table])
            assert len(exploration["rows"]) == 2
            for row in exploration["rows"]:
                assert row

    @staticmethod
    def verify_schema_structure(db_schema):
        assert isinstance(db_schema, dict)
        assert len(db_schema) > 0
        for table, rows in db_schema.items():
            assert isinstance(table, str)
            assert isinstance(rows, list)
            assert len(rows) > 0
            for row in rows:
                assert isinstance(row, str)
