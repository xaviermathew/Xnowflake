from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

from xnowflake.settings import settings
from xnowflake.tables.tables import TableUtil, TableWriter, Table


class Session(object):
    def __init__(self):
        conf = SparkConf()\
            .set('spark.executor.cores', settings.NUM_EXECUTORS) \
            .set('spark.executor.memory', settings.MAX_MEMORY)\
            .set('spark.driver.memory', settings.MAX_MEMORY)
        self.sc = SparkContext(conf=conf)
        self.sqlContext = SQLContext(self.sc)

    @property
    def tables(self):
        return TableUtil(self)

    def register_table(self, name, df, schema):
        t = Table(self, name, schema)
        t.create(df)
