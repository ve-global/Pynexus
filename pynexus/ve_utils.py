import functools
import time
import zipfile
from io import BytesIO

from .logs import logger

DEFAULT_FMT = '[{name}] {elapsed:0.8f} min'


def clock(fmt=DEFAULT_FMT):
    """
    Computes and prints the duration of the function
    :param fmt: format of the time.
    :return:
    """

    def decorate(func):
        @functools.wraps(func)
        def clocked(*args, **kwargs):
            t0 = time.time()
            _result = func(*args, **kwargs)
            elapsed = (time.time() - t0) / 60.
            name = func.__name__
            args = ', '.join(repr(arg) for arg in args)
            result = repr(_result)
            logger.info(fmt.format(**locals()))
            return _result
        return clocked
    return decorate


def is_notebook():
    """
    Test if running in notebook
    :return:
    """
    try:
        from traitlets import TraitError
        from ipywidgets import IntProgress
    except ImportError:
        return False

    try:
        IntProgress()
    except TraitError:
        return False
    else:
        return True


def get_chunks(l, n):
    """Chunk a list `l` in chunks of size `n`"""
    for x in range(0, len(l), n):
        yield l[x: x + n]


def zip_files(files):
    """Zip files in memory"""
    zipped_file = BytesIO()
    with zipfile.ZipFile(zipped_file, 'w') as zip:
        for file_name, file in files.items():
            file.seek(0)
            zip.writestr("{}.csv".format(file_name), file.read())

    zipped_file.seek(0)
    return zipped_file
