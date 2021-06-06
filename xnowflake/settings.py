import importlib
import logging
import multiprocessing
import warnings


def get_object_from_python_path(python_path):
    parts = python_path.split('.')
    class_name = parts.pop(-1)
    mod_path = '.'.join(parts)
    mod = importlib.import_module(mod_path)
    return getattr(mod, class_name)


ROOT_PATH = '/tmp/xnowflake/'
TABLE_STORAGE_PATH = ROOT_PATH + 'tables/'
TMP_PATH = ROOT_PATH + 'tmp/'
MAX_ROWS_PER_CHUNK = 10 * 1000

NUM_EXECUTORS = multiprocessing.cpu_count()
MAX_MEMORY = 2 * 1024 * 1024 * 1024

LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s:%(levelname)s:%(filename)s:%(lineno)d:%(funcName)s():%(message)s'


class Settings(object):
    def __getattr__(self, item):
        v = globals()[item]
        if isinstance(v, str) and v.startswith('xnowflake.'):
            v = get_object_from_python_path(v)
        return v

try:
    from .local import *
except ImportError:
    try:
        from .production import *
    except ImportError:
        warnings.warn('local.py/production.py is missing')


settings = Settings()
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
