import argparse
from dbutils import *
from fetcher import *

_DEFAULT_CENTRAL = 'wtw3sm0'
_DEFAULT_DEPTH = 65


def _parse_args():
    """
    :return: argparse.parse_args
    """
    parse = argparse.ArgumentParser(description='ele.me spider v2.0')
    parse.add_argument('-d', '--db_name', help='Continuous task for database', dest='db_name')
    parse.add_argument('-c', '--central', help='Central geohash', dest='central')
    parse.add_argument('-p', '--depth', help='Depth of searching', dest='depth')
    return parse.parse_args()


if __name__ == '__main__':
    args = _parse_args()

    db_name = db_utils.create_database(_DEFAULT_CENTRAL, _DEFAULT_DEPTH)
    worker = worker.Worker(db_name)
    worker.run()
