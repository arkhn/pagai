import datetime

from pagai.services.database_explorer import DatabaseExplorer, POSTGRES


class TestExploration:
    def test_explore(self, db_config):
        explorer = DatabaseExplorer(db_config["handler"], db_config)
        exploration = explorer.explore(table="patients", limit=2)

        assert exploration["fields"] == ["index", "patient_id", "gender", "date"]
        assert sorted(exploration["rows"]) == [
            [0, 1, "F", datetime.datetime(1974, 3, 5, 0, 0)],
            [1, 2, "M", datetime.datetime(1969, 12, 21, 0, 0)],
        ]

    def test_owners(self, db_config):
        explorer = DatabaseExplorer(db_config["handler"], db_config)
        owners = explorer.get_owners()
        if db_config["handler"] == "postgresql":
            assert owners == [
                "pg_toast",
                "pg_temp_1",
                "pg_toast_temp_1",
                "pg_catalog",
                "public",
                "information_schema",
            ]
        else:
            assert owners == [
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
            ]

    def test_get_db_schema_(self, db_config):
        explorer = DatabaseExplorer(db_config["handler"], db_config)
        if db_config["handler"] == "postgresql":
            db_schema = explorer.get_db_schema(owner="public")
            assert isinstance(db_schema, dict)
        else:
            db_schema = explorer.get_db_schema(owner="SYSTEM")
            assert isinstance(db_schema, dict)
