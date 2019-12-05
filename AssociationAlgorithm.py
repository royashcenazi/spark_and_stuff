from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import Row

DATA_TABLE = "input_data"
SUPPORT_TABLE = "individual_support"
UNION_SUPPORT_TABLE = "union_support"
UNIQUE_VAL_TABLE = "unique_values"


class AssociationAlgorithm:
    def __init__(self, df, tr_id, tr_val, sqlctx):
        self.df = df
        self.id_col_name = tr_id
        self.val_col_name = tr_val
        self.sqlctx = sqlctx
        self.df.registerTempTable(DATA_TABLE)

    def execute_algorithm(self):
        self.create_unique_val_table()

        self.create_support_table()

        self.create_union_supp_table()

        self.create_conf_table()

        self.writeData()

    def create_unique_val_table(self):
        self.sqlctx.sql(
            """
            select distinct {} 
            from {}
            """.format(self.val_col_name, DATA_TABLE)
        ).registerTempTable(UNIQUE_VAL_TABLE)

    def create_support_table(self, starting_index=0):
        df = self.sqlctx.sql("""select {}, count(*) as support
                            from {}
                            group by {}
                            """.format(self.val_col_name, DATA_TABLE, self.val_col_name))

        df.registerTempTable(SUPPORT_TABLE)

    def create_union_supp_table(self):
        df = self.sqlctx.sql(
            """
            select a.{} val1, b.{} val2, count(*) as union_support
            from {} a, {} b 
            where a.{} = b.{}
            group by a.{}, b.{}
            having a.{} > b.{}
            """.format(self.val_col_name, self.val_col_name, DATA_TABLE, DATA_TABLE, self.id_col_name,
                       self.id_col_name, self.val_col_name,
                       self.val_col_name, self.val_col_name, self.val_col_name))

        df.registerTempTable(UNION_SUPPORT_TABLE)

    def create_conf_table(self):
        df = self.sqlctx.sql(
            """
            select a.val1, a.val2, first(a.union_support/b.support) conf_ab, first(a.union_support/c.support) conf_ba
            from union_support a, individual_support b, individual_support c
            where a.val1 = b.name
            and a.val2 = c.name
            group by a.val1, a.val2
            having a.val1 > a.val2
            """
        )

        df.registerTempTable("conf")

    def writeData(self):
        df = self.sqlctx.sql(
            """select * 
            from conf 
            where (conf_ab > 0.8 
            and conf_ab < 1)
            or
            (conf_ba > 0.8 
            and conf_ba < 1)
            """)

        df.coalesce(1).write.csv("confidence.csv")


def main():
    sc = SparkContext()
    sqlctx = SQLContext(sc)

    lines = sc.textFile("alerts.csv").map(lambda l: l.split(","))
    alerts_rdd = lines.map(lambda l: Row(ts=l[1], name=l[0]))
    df = sqlctx.createDataFrame(alerts_rdd)

    aa = AssociationAlgorithm(df, 'ts', 'name', sqlctx)
    aa.execute_algorithm()


main()
