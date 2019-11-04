from pagai.engine.structure.table import Table


class Graph:
    def __init__(self):
        self.table_dict = {}
        self.num_tables = 0

    def __iter__(self):
        return iter(self.table_dict.values())

    def get_distance(self, table1_name, table2_name):
        """
        Get distance in the join graph. The method is not recursive as
        the graph is not related/"connexe", and max distance is 3.
        """
        if table1_name == table2_name:
            return 0
        table1 = self.get_table(table1_name)
        table2 = self.get_table(table2_name)

        if table2 in table1.get_joins():
            return 1
        else:
            for neighbor in table1.get_joins():
                if table2 in neighbor.get_joins():
                    return 2
        return 3

    def add_table(self, table_name):
        self.num_tables = self.num_tables + 1
        new_table = Table(table_name)
        self.table_dict[table_name] = new_table
        return new_table

    def get_table(self, name):
        if name in self.table_dict:
            return self.table_dict[name]
        else:
            return None

    def add_join(self, frm, to, join_info):
        if frm not in self.table_dict:
            self.add_table(frm)
        if to not in self.table_dict:
            self.add_table(to)

        self.table_dict[frm].add_join(self.table_dict[to], join_info)
        # self.table_dict[to].add_join(self.table_dict[frm], join_info)

    def get_tables(self):
        return self.table_dict.keys()
