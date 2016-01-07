import datetime
import sqlite3
import sys
from itertools import *

import geohash

from fetcher import *

_MAJOR_CATEGORY_TEXT = {
    207: '全部快餐类',
    220: '全部正餐',
    233: '全部零食',
    239: '全部甜品饮品',
    248: '全部蛋糕',
}

_MINOR_CATEGORY_TEXT = {
    208: '品牌快餐',
    209: '盖浇饭',
    210: '中式炒菜',
    211: '披萨意面',
    212: '汉堡',
    213: '米粉面馆',
    214: '麻辣烫',
    215: '包子粥店',
    216: '生煎锅贴',
    217: '饺子馄饨',
    218: '烧烤',
    219: '香锅',
    221: '川湘菜',
    222: '粤菜',
    223: '东北菜',
    224: '云南菜',
    225: '江浙菜',
    226: '西北菜',
    227: '鲁菜',
    228: '清真',
    229: '日韩料理',
    230: '西餐',
    231: '火锅',
    232: '海鲜',
    234: '炸鸡炸串',
    235: '鸭脖卤味',
    236: '小龙虾',
    237: '地方小吃',
    238: '零食',
    240: '饮品',
    241: '甜品',
    242: '咖啡',
    243: '点心',
    249: '蛋糕',
    250: '面包'
}


class _MapGridIterator():
    def __init__(self, central, depth=65):
        self._cells = set()
        self._next_batch = set([central])
        self._computed_cells = set()
        self.max_depth = depth
        self.current_depth = 0
        self._refresh_output()

    def __iter__(self):
        return self

    def __next__(self):
        cell = self._next_cell()
        if cell is None:
            raise StopIteration
        return cell,

    def _add_neighbors(self, cell):
        if cell in self._computed_cells:
            return

        n = geohash.neighbors(cell)

        def cond(c): return (c in self._computed_cells) or (c in self._cells)

        n[:] = list(filterfalse(cond, n))

        self._next_batch.update(n)
        self._computed_cells.add(cell)

    def _advance_depth(self):
        self._cells = self._next_batch
        self._next_batch = set()
        self.current_depth += 1
        self._refresh_output()

    def _take_cell(self):
        if len(self._cells) == 0:
            return None
        cell = self._cells.pop()
        self._add_neighbors(cell)
        return cell

    def _refresh_output(self):
        sys.stdout.write("\r创建地图网格(深度:%d) %.2f%%" % (self.max_depth, self.current_depth / self.max_depth * 100.0))
        sys.stdout.flush()

    def _next_cell(self):
        if len(self._cells) == 0 and self.current_depth < self.max_depth:
            self._advance_depth()

        return self._take_cell()


def _create_status_table(conn, central, depth):
    """
    Create geohash-grid table
    """
    cursor = conn.cursor()
    cursor.executescript('''
        DROP TABLE IF EXISTS grid;
        CREATE TABLE grid
            (
            geohash CHARACTER(7) PRIMARY KEY NOT NULL,
            fetch_status TINYINT DEFAULT 0,
            commit_date DATETIME
            );

        DROP TABLE IF EXISTS restaurants;
        CREATE TABLE restaurants
            (
            id INTEGER PRIMARY KEY NOT NULL,
            fetch_status TINYINT DEFAULT 0,
            commit_date DATETIME
            );
    ''')

    grid_iter = _MapGridIterator(central, depth)

    cursor.executemany('''INSERT INTO grid(geohash) VALUES (?);''', grid_iter)
    conn.commit()
    print('')


def _create_data_table(conn):
    cursor = conn.cursor()
    cursor.executescript('''
        DROP TABLE IF EXISTS restaurants;
        CREATE TABLE restaurants
            (
            id INTEGER PRIMARY KEY NOT NULL,
            name VARCHAR(128) NOT NULL,
            name_for_url VARCHAR(32) NOT NULL,
            rating TINYINT,
            rating_count INTEGER,
            month_sales INTEGER,
            phone VARCHAR(16),
            latitude REAL,
            longitude REAL,
            is_free_delivery BOOLEAN,
            delivery_fee REAL,
            minimum_order_amount REAL,
            minimum_free_delivery_amount REAL,
            promotion_info TEXT,
            address TEXT
            );

        DROP TABLE IF EXISTS menus;
        CREATE TABLE menus
            (
            id INTEGER PRIMARY KEY NOT NULL,
            restaurant_id INTEGER NOT NULL,
            name VARCHAR(128) NOT NULL,
            pinyin_name VARCHAR(128),
            rating TINYINT,
            rating_count INTEGER,
            price REAL,
            month_sales INTEGER,
            description TEXT,
            category_id INTEGER,
            specfoods_json TEXT
            );

        CREATE INDEX restaurant_id_idx ON menus(restaurant_id);
    ''')
    conn.commit()
    print('创建商家数据库...完成')





def _create_categery_table(conn):
    cursor = conn.cursor()
    cursor.executescript('''
        DROP TABLE IF EXISTS category;
        CREATE TABLE category
            (
            id INTEGER PRIMARY KEY NOT NULL,
            name VARCHAR(16) NOT NULL,
            major_id INTEGER NOT NULL,
            major_name VARCHAR(16) NOT NULL
            );
    ''')

    for major, minors in worker.RESTAURANT_CATEGORIES.items():
        for minor in minors:
            cursor.execute('INSERT INTO category VALUES(?,?,?,?)',
                           (minor,
                            _MINOR_CATEGORY_TEXT[minor],
                            major,
                            _MAJOR_CATEGORY_TEXT[major]
                            ))

    cursor.executescript('''
        DROP TABLE IF EXISTS restaurant_categories;
        CREATE TABLE restaurant_categories
            (
            category_id INTEGER NOT NULL,
            restaurant_id INTEGER NOT NULL
            );
    ''')
    conn.commit()
    print('创建分类数据库...完成')


def _create_log_table(conn):
    cursor = conn.cursor()
    cursor.executescript('''
        DROP TABLE IF EXISTS fetch_restaurant_log;
        CREATE TABLE fetch_restaurant_log
            (
            geohash CHARACTER(7) NOT NULL,
            http_status_code SMALLINT NOT NULL,
            error_message TEXT
            );

        DROP TABLE IF EXISTS fetch_restaurant_exception;
        CREATE TABLE fetch_restaurant_exception
            (
            geohash CHARACTER(7) NOT NULL,
            exception TEXT
            );

        DROP TABLE IF EXISTS fetch_menu_log;
        CREATE TABLE fetch_menu_log
            (
            restaurant_id INTEGER NOT NULL,
            http_status_code SMALLINT NOT NULL,
            error_message TEXT
            );

        DROP TABLE IF EXISTS fetch_menu_exception;
        CREATE TABLE fetch_menu_exception
            (
            restaurant_id INTEGER NOT NULL,
            exception TEXT
            );
    ''')
    conn.commit()
    print('创建日志数据库...完成')


def create_db_name_dict(date=None):
    date_part = date if date is not None else datetime.datetime.now().strftime("%Y-%m-%d")
    return {
        'status': date_part + '-status.db',
        'data': date_part + '-data.db',
        'log': date_part + '-log.db',
    }


def create_database(central, depth):
    db_names = create_db_name_dict()
    print('初始化数据库:\n状态数据:"{}"\n商家数据:"{}"\n日志数据:"{}"...'.format(
            db_names['status'], db_names['data'], db_names['log']))
    with sqlite3.connect(db_names['status'], isolation_level='EXCLUSIVE') as conn:
        _create_status_table(conn, central, depth)

    with sqlite3.connect(db_names['data'], isolation_level='EXCLUSIVE') as conn:
        _create_data_table(conn)
        _create_categery_table(conn)

    with sqlite3.connect(db_names['log'], isolation_level='EXCLUSIVE') as conn:
        _create_log_table(conn)

    print('数据库初始化完成')
    return db_names


def prepare_restaurant_status_table(db_names):
    with sqlite3.connect(db_names['data'],isolation_level='EXCLUSIVE') as data_conn:
        data_curosr = data_conn.cursor()
        data_rows = data_curosr.execute('select id from restaurants')

        with sqlite3.connect(db_names['status'],isolation_level='EXCLUSIVE') as status_conn:
            status_cursor = status_conn.cursor()
            for row in data_rows:
                status_cursor.execute('''INSERT INTO restaurants(id) VALUES(?);''', row)
            status_conn.commit()


