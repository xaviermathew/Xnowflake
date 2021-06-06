import os

from xnowflake.settings import settings


class DatasetWriter(object):
    def __init__(self, ctx, name):
        self.ctx = ctx
        self.name = name

    @staticmethod
    def _get_fname(name, purpose, part_id=None, col=None, value=None, other=None):
        parts = [settings.TABLE_STORAGE_PATH, name]
        if purpose is not None:
            parts.append(purpose)
        if col is not None:
            parts.append(col)
        if value is not None:
            parts.append(str(value))
        if part_id:
            parts.append(str(part_id))
        if other:
            if isinstance(other, list):
                parts.extend(other)
            else:
                parts.append(other)
        parts = map(str, parts)
        return os.path.join(*parts)

    def get_fname(self, purpose, part_id=None, col=None, value=None, other=None):
        return self._get_fname(self.name, purpose, part_id=part_id, col=col, value=value, other=other)

    @staticmethod
    def _write(fname, df):
        path = os.path.join(settings.TABLE_STORAGE_PATH, fname)
        dir_name = os.path.dirname(path)
        os.makedirs(dir_name, exist_ok=True)
        df.to_parquet(fname)

    def write(self, df):
        self._write(self.name, df)
