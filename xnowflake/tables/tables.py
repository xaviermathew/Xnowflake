import logging
import os
import time

import pandas as pd

from xnowflake.tables import DatasetWriter
from xnowflake.settings import settings

_LOG = logging.getLogger(__name__)


class TableUtil(object):
    def __init__(self, ctx):
        self.ctx = ctx

    def list(self):
        tables = {}
        for dir_name in os.listdir(settings.TABLE_STORAGE_PATH):
            dir_path = os.path.join(settings.TABLE_STORAGE_PATH, dir_name)
            if os.path.isdir(dir_path):
                t = Table(self.ctx, dir_name)
                tables[t.name] = t
        return tables

    def get(self, name):
        return self.list()[name]


def _create_categorical_col_index(table_name, group_chunk, id_cols, cat_cols, i):
    other = '%s_%s.parquet' % (int(time.time()), i)
    fname = DatasetWriter._get_fname(table_name, purpose='categorical_col_index', other=other)
    id_chunk = group_chunk[id_cols].reset_index(drop=True)
    DatasetWriter._write(fname, id_chunk)
    # register chunk in db
    _LOG.debug('generated cat index with [%s] rows for combination:[%s] at [%s]',
               len(id_chunk), id_chunk[:1].values, fname)


def _create_id_col_index(table_name, group_chunk, id_col, pk_col, i):
    hash_value = group_chunk['__id_hash'][0]
    fname = DatasetWriter._get_fname(table_name, purpose='id_col_index', col=id_col, value=hash_value)
    id_chunk = group_chunk[[id_col, pk_col]].reset_index(drop=True).sort_values(id_col)
    DatasetWriter._write(fname, id_chunk)
    # register chunk in db
    _LOG.debug('generated id index with [%s] rows for hash_value:[%s] at [%s]',
               len(id_chunk), hash_value, fname)


def create_or_update_indices(table_name, schema, chunk, i):
    chunk = pd.DataFrame(chunk, columns=schema.col_names)
    id_cols = schema.id_col_names
    pk_col = schema.pk_col_name
    cat_cols = schema.categorical_col_names
    chunk.groupby(schema.categorical_col_names).apply(lambda x: _create_categorical_col_index(table_name, x, id_cols, cat_cols, i))
    for col in schema.other_id_cols:
        id_chunk = chunk[[col.name, schema.pk_col_name]]
        id_chunk['__id_hash'] = id_chunk[col.name].apply(col.hash_fn)
        id_chunk.groupby('__id_hash').apply(lambda x: _create_id_col_index(table_name, x, col, pk_col, i))
    return []


class TableWriter(DatasetWriter):
    def __init__(self, table, schema):
        self.table = table
        super(TableWriter, self).__init__(table.ctx, table.name)
        self.schema = schema

    def create(self, df):
        if self.schema.pk_col_name:
            df = df.sortWithinPartitions(self.schema.pk_col_name)
        if self.schema.partition_col_names:
            df = df.partitionBy(*self.schema.partition_col_names)
        table_dir = self.get_fname(purpose='table')
        df.write.option('maxRecordsPerFile', settings.MAX_ROWS_PER_CHUNK)\
                .parquet(table_dir)

        table_name = self.table.name
        schema = self.schema
        df.rdd.mapPartitionsWithIndex(lambda i, chunk: create_or_update_indices(table_name, schema, chunk, i)).collect()

    def insert(self, df):
        raise NotImplementedError


class Table(object):
    def __init__(self, ctx, name, schema):
        self.ctx = ctx
        self.name = name
        self.schema = schema
        self.path = os.path.join(settings.TABLE_STORAGE_PATH, name)

    def __repr__(self):
        return '<Table:%s>' % self.name

    def save(self):
        pass

    def create(self, df):
        self.save()
        TableWriter(self, self.schema).create(df)

    def insert(self, df):
        TableWriter(self, self.schema).insert(df)
