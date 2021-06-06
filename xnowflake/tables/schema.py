from functools import cached_property


class Col(object):
    def __init__(self, name, is_continuous=False):
        self.name = name
        self.is_continuous = is_continuous

    def __repr__(self):
        return '<%s:%s>' % (self.__class__.__name__, self.name)


class Categorical(Col):
    pass


class ID(Col):
    def __init__(self, name, hash_fn=None, is_increasing=False, **kwargs):
        self.hash_fn = hash_fn
        self.is_increasing = is_increasing
        super(ID, self).__init__(name, **kwargs)


class Schema(object):
    def __init__(self, cols, partition_cols=None):
        self.cols = cols
        self.col_names = [col.name for col in cols]

        if partition_cols is None:
            partition_cols = []
        col_map = {col.name: col for col in cols}
        self.partition_cols = [col_map[col] for col in partition_cols]
        self.partition_col_names = partition_cols
        self.categorical_cols = [col for col in self.cols if isinstance(col, Categorical)]
        self.categorical_col_names = [col.name for col in self.categorical_cols]
        self.id_cols = [col for col in self.cols if isinstance(col, ID)]
        self.id_col_names = [col.name for col in self.id_cols]

        self.pk_col = None
        self.pk_col_name = None
        for col in self.id_cols:
            if col.is_increasing:
                self.pk_col = col
                self.pk_col_name = col.name

        self.other_id_cols = [col for col in self.id_cols if col != self.pk_col]
        self.other_id_col_names = [col.name for col in self.other_id_cols]
