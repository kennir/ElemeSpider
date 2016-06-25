import argparse

from analyzer import *
from dbutils import *
from fetcher import *
import geohash

_DEFAULT_CENTRAL = 'wtw3sm0'
_DEFAULT_DEPTH = 65


_CENTRAL_SEQUENCE = ['wtw3esj', 'wtw3ef9', 'wtw3syu']
_CENTRAL_SEQUENCE_DEPTH = 40


_LIMIT_LONGLAT = [[31.2243287344,121.450360246], [31.2152904,121.4564706], [31.2384794,121.5033301]]



def _parse_args():
    """
    :return: argparse.parse_args
    """
    parse = argparse.ArgumentParser(description='ele.me spider v2.0')
    parse.add_argument('-d', '--db_name', help='Continuous task for database', dest='db_name')
    parse.add_argument('-a', '--analysis', help="Analysis only", dest='analysis')
    parse.add_argument('-l', '--limition', help='Limit range',action='store_true')
    parse.add_argument('-c', '--central', help='Central geohash', dest='central')
    parse.add_argument('-p', '--depth', help='Depth of searching', dest='depth', type=int)
    return parse.parse_args()


def fetch_restaurants(db_names):
    restaurant_fetcher = worker.ProcessingLauncher(db_names, worker.fetch_restaurant_processor)
    restaurant_fetcher.run()
    # return db_names

def fetch_menus(db_names):
    db_utils.prepare_restaurant_status_table(db_names)
    menu_fetcher = worker.ProcessingLauncher(db_names, worker.fetch_menu_processor)
    menu_fetcher.run()



def start_new_mission_sequence():
    db_name_sequence = db_utils.create_database_sequence(_CENTRAL_SEQUENCE, _CENTRAL_SEQUENCE_DEPTH)
    for db_names in db_name_sequence:
        fetch_restaurants(db_names)
    for db_names in db_name_sequence:
        fetch_menus(db_names)



def start_analysis_mission(db_name, limition=False):
    print('开始分析数据:', db_name)

    lon = None
    lat = None

    if limition is True:
        lat,lon = geohash.decode(db_name)

    analyzer = topline.Analyzer(db_name, lon, lat, 1.5)
    analyzer.generate()


if __name__ == '__main__':
    args = _parse_args()

    if args.analysis is not None:
        start_analysis_mission(args.analysis, False if not args.limition else True)
    elif args.central is not None and args.depth is not None:
        db_name_sequences = db_utils.create_database_sequence([args.central], args.depth)
        fetch_restaurants(db_name_sequences[0])
        fetch_menus(db_name_sequences[0])
    else:
        start_new_mission_sequence()

    # elif args.db_name is not None:
        # pass
    # else:
        # db_names = start_new_mission()
        # start_analysis_mission(db_names['date'])
