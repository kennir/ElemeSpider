import json
import multiprocessing
import os
import sys
import threading

import requests

from dbutils import db_utils
from fetcher import url_utils

_REQUEST_TIMEOUT = 3

# 207 全部快餐类
# 220 全部正餐类
# 233 小吃零食
# 239 甜品饮品
# 248 蛋糕
RESTAURANT_CATEGORIES = {
    207: [208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219],
    220: [221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232],
    233: [234, 235, 236, 237, 238],
    239: [240, 241, 242, 243],
    248: [249, 250]
}


class RestaurantFetcher(object):
    def __init__(self, db_names):
        self.db_names = db_names
        self.num_cells = self._num_cells()
        self.num_finished = 0
        self.num_restaurants = 0
        self._restaurant_cache = []
        self._category_cache = []

    def _log_http_error(self, geohash, http_code, error_msg):
        with db_utils.connect_database(self.db_names['log']) as conn:
            conn.execute('INSERT INTO fetch_restaurant_log VALUES(?,?,?)',
                         (geohash, http_code, error_msg))
            conn.commit()

    def _log_exception(self, geohash, exception):
        with db_utils.connect_database(self.db_names['log']) as conn:
            conn.execute('INSERT INTO fetch_restaurant_exception VALUES(?,?)',
                         (geohash, exception))
            conn.commit()

    def _take_geohash(self):
        with db_utils.connect_database(self.db_names['status'], isolation_level='EXCLUSIVE') as conn:
            cursor = conn.cursor()
            cursor.execute('BEGIN EXCLUSIVE')
            geohash = cursor.execute('SELECT geohash FROM grid WHERE fetch_status = 0 LIMIT 1').fetchone()
            if geohash is not None:
                cursor.execute('UPDATE grid SET fetch_status = 1 WHERE geohash = ?', geohash)
            conn.commit()
            return geohash

    def _finish_geohash(self, geohash):
        with db_utils.connect_database(self.db_names['status']) as conn:
            cursor = conn.cursor()
            cursor.execute(
                    '''UPDATE grid SET fetch_status = 2,commit_date = datetime('now','localtime') WHERE geohash = ?''',
                    (geohash,))

            conn.commit()
            row = cursor.execute('select count(*) from grid where fetch_status != 0 and fetch_status != 1').fetchone()
            if row is not None:
                self.num_finished = row[0]

        with db_utils.connect_database(self.db_names['data']) as conn:
            row = conn.execute('SELECT COUNT(*) FROM restaurants').fetchone()
            if row is not None:
                self.num_restaurants = row[0]
        self._refresh_output()

    def _refresh_output(self):
        sys.stdout.write("\r抓取地图网格商家数据(%d/%d) %.2f%% 商家数:%d pid:%d" %
                         (self.num_finished, self.num_cells,
                          self.num_finished / self.num_cells * 100.0,
                          self.num_restaurants,
                          os.getpid()))
        sys.stdout.flush()

    def _store_restaurants(self, geohash, minor_cat, restaurants):
        restaurants_json = json.loads(restaurants)
        for r_json in restaurants_json:
            self._restaurant_cache.append((
                r_json['id'],
                r_json['name'],
                r_json['name_for_url'],
                r_json['rating'],
                r_json['rating_count'],
                r_json['month_sales'],
                r_json['phone'],
                r_json['latitude'],
                r_json['longitude'],
                r_json['is_free_delivery'],
                r_json['delivery_fee'],
                r_json['minimum_order_amount'],
                r_json['minimum_free_delivery_amount'],
                r_json['promotion_info'],
                r_json['address']
            ))
            self._category_cache.append((
                minor_cat,
                r_json['id'],
                minor_cat,
                r_json['id'],
            ))

    def _fetch_cell_category(self, geohash, minor_cat):
        while True:
            try:
                r = requests.get(url_utils.create_fetch_restaurant_url(geohash, minor_cat), timeout=_REQUEST_TIMEOUT)
                if r.status_code == requests.codes.ok:
                    self._store_restaurants(geohash, minor_cat, r.text)
                    break
                else:
                    self._log_http_error(geohash, r.status_code, r.text)
            except Exception as e:
                self._log_exception(geohash, str(e))
                continue

    def _num_cells(self):
        with db_utils.connect_database(self.db_names['status']) as conn:
            row = conn.execute('SELECT COUNT(*) FROM grid').fetchone()
            return row[0] if row is not None else 0

    def _write_cache_to_database(self):
        with db_utils.connect_database(self.db_names['data']) as conn:
            cursor = conn.cursor()
            cursor.executemany('''
                INSERT OR IGNORE INTO restaurants VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''', self._restaurant_cache)
            cursor.executemany('''
                INSERT INTO restaurant_categories(category_id,restaurant_id)
                SELECT ?,?
                WHERE NOT EXISTS(SELECT 1 FROM restaurant_categories WHERE category_id = ? AND restaurant_id = ?)
                ''', self._category_cache)
            conn.commit()
        self._restaurant_cache = []
        self._category_cache = []

    def _fetch_cell(self, geohash):
        for major, minors in RESTAURANT_CATEGORIES.items():
            for minor in minors:
                self._fetch_cell_category(geohash, minor)
        self._write_cache_to_database()
        self._finish_geohash(geohash)

    def run(self):
        geohash = self._take_geohash()
        while geohash is not None:
            self._fetch_cell(geohash[0])
            geohash = self._take_geohash()


class MenuFetcher(object):
    def __init__(self, db_names):
        self.db_names = db_names
        self.num_restaurants = self._num_restaurants()
        self.num_finished = 0
        self.num_menus = 0
        self._menu_cache = []

    def _log_http_error(self, restaurant_id, http_code, error_msg):
        with db_utils.connect_database(self.db_names['log']) as conn:
            conn.execute('INSERT INTO fetch_menu_log VALUES(?,?,?)',
                         (restaurant_id, http_code, error_msg))
            conn.commit()

    def _log_exception(self, restaurant_id, exception):
        with db_utils.connect_database(self.db_names['log']) as conn:
            conn.execute('INSERT INTO fetch_menu_exception VALUES(?,?)',
                         (restaurant_id, exception))
            conn.commit()

    def _refresh_output(self):
        sys.stdout.write("\r抓取菜单数据(%d/%d) %.2f%% 菜单数:%d pid:%d" %
                         (self.num_finished, self.num_restaurants,
                          self.num_finished / self.num_restaurants * 100.0,
                          self.num_menus,
                          os.getpid()))
        sys.stdout.flush()

    def _num_restaurants(self):
        with db_utils.connect_database(self.db_names['status']) as conn:
            row = conn.execute('SELECT COUNT(*) FROM restaurants').fetchone()
            return row[0] if row is not None else 0

    def _take_restaurant(self):
        with db_utils.connect_database(self.db_names['status'], isolation_level='EXCLUSIVE') as conn:
            cursor = conn.cursor()
            cursor.execute('BEGIN EXCLUSIVE')
            row = cursor.execute('SELECT id FROM restaurants WHERE fetch_status = 0 LIMIT 1').fetchone()
            if row is not None:
                cursor.execute('UPDATE restaurants SET fetch_status = 1 WHERE id = ?', row)
            conn.commit()
            return row

    def _write_cache_to_database(self):
        with db_utils.connect_database(self.db_names['data']) as conn:
            conn.executemany('''
                INSERT INTO menus(restaurant_id,name,pinyin_name,rating,rating_count,price,month_sales,description,category_id)
                VALUES(?,?,?,?,?,?,?,?,?)
            ''', self._menu_cache)
            conn.commit()
        self._menu_cache = []

    def _finish_restaurant(self, restaurant_id, status_code=2):
        with db_utils.connect_database(self.db_names['status']) as conn:
            cursor = conn.cursor()
            cursor.execute(
                    '''UPDATE restaurants SET fetch_status = ?,commit_date = datetime('now','localtime') WHERE id = ?''',
                    (status_code, restaurant_id))
            conn.commit()
            row = cursor.execute(
                    'select count(*) from restaurants where fetch_status != 0 and fetch_status != 1').fetchone()
            if row is not None:
                self.num_finished = row[0]

        with db_utils.connect_database(self.db_names['data']) as conn:
            row = conn.execute('SELECT COUNT(*) FROM menus').fetchone()
            if row is not None:
                self.num_menus = row[0]
        self._refresh_output()

    @staticmethod
    def _sum_price(specfoods_json):
        price = 0
        for f_json in specfoods_json:
            price += float(f_json['price'])

        count = len(specfoods_json)
        if count is not 0:
            price /= count

        return price

    def _store_menus(self, restaurant_id, menus):
        menus_json = json.loads(menus)
        for menu_category_json in menus_json:  # 分类
            for food_json in menu_category_json['foods']:
                self._menu_cache.append((
                    food_json['restaurant_id'],
                    food_json['name'],
                    food_json['pinyin_name'],
                    food_json['rating'],
                    food_json['rating_count'],
                    self._sum_price(food_json['specfoods']),
                    food_json['month_sales'],
                    food_json['description'],
                    food_json['category_id'],
                ))

    def _fetch_restaurant(self, restaurant_id):
        while True:
            try:
                r = requests.get(url_utils.create_fetch_menu_url(restaurant_id), timeout=_REQUEST_TIMEOUT)
                if r.status_code == requests.codes.ok:
                    self._store_menus(restaurant_id, r.text)
                    self._finish_restaurant(restaurant_id)
                    break
                elif r.status_code == requests.codes.not_found:
                    self._log_http_error(restaurant_id, r.status_code, r.text)
                    self._finish_restaurant(restaurant_id, r.status_code)
                    break
                else:
                    self._log_http_error(restaurant_id, r.status_code, r.text)
            except Exception as e:
                self._log_exception(restaurant_id, str(e))

    def run(self):
        restaurant_id = self._take_restaurant()
        while restaurant_id is not None:
            self._fetch_restaurant(restaurant_id[0])
            self._write_cache_to_database()
            restaurant_id = self._take_restaurant()


def fetch_restaurant_threading(db_names):
    RestaurantFetcher(db_names).run()


def fetch_menu_threading(db_names):
    MenuFetcher(db_names).run()


def fetch_restaurant_processor(db_names, num_threading):
    print('进程%d已启动' % os.getpid())
    ThreadingLauncher(db_names, fetch_restaurant_threading, num_threading).run()
    print('\n进程%d已结束' % os.getpid())


def fetch_menu_processor(db_names, num_threading):
    print('进程%d已启动' % os.getpid())
    ThreadingLauncher(db_names, fetch_menu_threading, num_threading).run()
    print('\n进程%d已结束' % os.getpid())


class ThreadingLauncher(object):
    def __init__(self, db_names, target_func, num_threading=8):
        self.db_names = db_names
        self.target_func = target_func
        self.num_threading = num_threading

    def run(self):
        threads = []

        for n in range(0, self.num_threading):
            threads.append(threading.Thread(target=self.target_func, args=(self.db_names,)))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()


class ProcessingLauncher(object):
    def __init__(self, db_names, target_func, num_processing=2, num_threading=8):
        self.db_names = db_names
        self.target_func = target_func
        self.num_processing = num_processing
        self.num_threading = num_threading

    def run(self):
        processes = []

        for n in range(0, self.num_processing):
            processes.append(multiprocessing.Process(target=self.target_func,
                                                     args=(self.db_names, self.num_threading)))
        for processor in processes:
            processor.start()

        for processor in processes:
            processor.join()
