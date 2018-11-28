class Table:
    def __init__(self, table_name):
        self.name = table_name
        self.adjacent = {}

    def __str__(self):
        display = [str(self.name)]
        for table in self.adjacent:
            display.append("\t" + table.name)
            for join in self.adjacent[table]:
                display.append("\t\t" + "=".join(join))
        return "\n".join(display)

    def add_join(self, table, join_info):
        if table not in self.adjacent:
            self.adjacent[table] = []
        self.adjacent[table].append(join_info)

    def get_joins(self):
        return list(self.adjacent.keys())

    def get_id(self):
        return self.name

    def get_join(self, table):
        return self.adjacent[table]
