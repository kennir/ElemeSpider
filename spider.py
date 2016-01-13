import argparse

from analyzer import *
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
    parse.add_argument('-a', '--analysis', help="Analysis only", dest='analysis')
    parse.add_argument('-c', '--central', help='Central geohash', dest='central')
    parse.add_argument('-p', '--depth', help='Depth of searching', dest='depth', type=int)
    return parse.parse_args()


def start_new_mission():
    db_names = db_utils.create_database(central, depth)

    restaurant_fetcher = worker.ProcessingLauncher(db_names, worker.fetch_restaurant_processor)
    restaurant_fetcher.run()

    db_utils.prepare_restaurant_status_table(db_names)

    menu_fetcher = worker.ProcessingLauncher(db_names, worker.fetch_menu_processor)
    menu_fetcher.run()





def start_analysis_mission(date):
    print('开始分析数据:', date)
    db_names = db_utils.create_db_name_dict(date)
    analyzer = topline.Analyzer(db_names['data'])
    analyzer.generate()


if __name__ == '__main__':
    args = _parse_args()

    central = _DEFAULT_CENTRAL if args.central is None else args.central
    depth = _DEFAULT_DEPTH if args.depth is None else args.depth

    if args.analysis is not None:
        start_analysis_mission(args.analysis)
    elif args.db_name is not None:
        pass
    else:
        start_new_mission()
