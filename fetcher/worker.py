import json
import multiprocessing
import os
import sqlite3
import sys
import threading

import requests

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

    def _log_http_error(self, geohash, http_code, error_msg):
        with sqlite3.connect(self.db_names['log']) as conn:
            conn.execute('INSERT INTO fetch_restaurant_log VALUES(?,?,?)',
                         (geohash, http_code, error_msg))
            conn.commit()

    def _log_exception(self, geohash, exception):
        with sqlite3.connect(self.db_names['log']) as conn:
            conn.execute('INSERT INTO fetch_restaurant_exception VALUES(?,?)',
                         (geohash, exception))
            conn.commit()

    def _take_geohash(self):
        with sqlite3.connect(self.db_names['status'], timeout=30, isolation_level='EXCLUSIVE') as conn:
            cursor = conn.cursor()
            cursor.execute('BEGIN EXCLUSIVE')
            geohash = cursor.execute('SELECT geohash FROM grid WHERE fetch_status = 0 LIMIT 1').fetchone()
            if geohash is not None:
                cursor.execute('UPDATE grid SET fetch_status = 1 WHERE geohash = ?', geohash)
            conn.commit()
            return geohash

    def _finish_geohash(self, geohash):
        with sqlite3.connect(self.db_names['status'], timeout=30) as conn:
            cursor = conn.cursor()
            cursor.execute(
                    '''UPDATE grid SET fetch_status = 2,commit_date = datetime('now','localtime') WHERE geohash = ?''',
                    (geohash,))

            conn.commit()
            row = cursor.execute('select count(*) from grid where fetch_status != 0 and fetch_status != 1').fetchone()
            if row is not None:
                self.num_finished = row[0]

            self._refresh_output()

    def _refresh_output(self):
        sys.stdout.write("\r抓取地图网格商家数据(%d/%d) %.2f%% 商家数:%d pid:%d" %
                         (self.num_finished, self.num_cells,
                          self.num_finished / self.num_cells * 100.0,
                          self.num_restaurants,
                          os.getpid()))
        sys.stdout.flush()

    def _store_restaurants(self, geohash, minor_cat, restaurants):
        with sqlite3.connect(self.db_names['data']) as conn:
            cursor = conn.cursor()

            restaurants_json = json.loads(restaurants)
            for r_json in restaurants_json:
                cursor.execute('''
                        INSERT OR IGNORE INTO restaurants VALUES
                        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ''', (
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

                cursor.execute('''
                        INSERT INTO restaurant_categories(category_id,restaurant_id)
                        SELECT ?,?
                        WHERE NOT EXISTS(SELECT 1 FROM restaurant_categories WHERE category_id = ? AND restaurant_id = ?)
                    ''', (
                    minor_cat,
                    r_json['id'],
                    minor_cat,
                    r_json['id'],
                ))

                row = cursor.execute('SELECT COUNT(*) FROM restaurants').fetchone()
                if row is not None:
                    self.num_restaurants = row[0]
                    self._refresh_output()
            conn.commit()

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
        with sqlite3.connect(self.db_names['status'], timeout=30) as conn:
            row = conn.execute('SELECT COUNT(*) FROM grid').fetchone()
            return row[0] if row is not None else 0

    def _fetch_cell(self, geohash):
        for major, minors in RESTAURANT_CATEGORIES.items():
            for minor in minors:
                self._fetch_cell_category(geohash, minor)
        self._finish_geohash(geohash)

    def run(self):
        geohash = self._take_geohash()
        while geohash is not None:
            self._fetch_cell(geohash[0])
            geohash = self._take_geohash()


def _fetch_restaurant_threading_worker(db_names):
    # print('线程%s已启动' % threading.current_thread().name)
    RestaurantFetcher(db_names).run()


class RestaurantFetchThreading(object):
    def __init__(self, db_names, num_threading=8):
        self.db_names = db_names
        self.num_threading = num_threading

    def run(self):
        threads = []
        for n in range(0, self.num_threading):
            threads.append(threading.Thread(target=_fetch_restaurant_threading_worker, args=(self.db_names,)))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()


def _fetch_restaurant_processing_worker(db_names):
    print('进程%d已启动' % os.getpid())
    RestaurantFetchThreading(db_names).run()


class RestaurantFetchProcessing(object):
    def __init__(self, db_names, num_processing=2):
        self.db_names = db_names
        self.num_processing = num_processing

    def run(self):
        processes = []
        for n in range(0, self.num_processing):
            processes.append(multiprocessing.Process(target=_fetch_restaurant_processing_worker, args=(self.db_names,)))

        print('开始抓取商家数据...')
        for processor in processes:
            processor.start()

        for processor in processes:
            processor.join()

        print('\n抓取商家数据完成')


class MenuFetcher(object):
    def __init__(self, db_names):
        self.db_names = db_names
        self.num_restaurants = self._num_restaurants()
        self.num_finished = 0
        self.num_menus = 0

    def _log_http_error(self, restaurant_id, http_code, error_msg):
        with sqlite3.connect(self.db_names['log']) as conn:
            conn.execute('INSERT INTO fetch_menu_log VALUES(?,?,?)',
                         (restaurant_id, http_code, error_msg))
            conn.commit()

    def _log_exception(self, restaurant_id, exception):
        with sqlite3.connect(self.db_names['log']) as conn:
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
        with sqlite3.connect(self.db_names['status'], timeout=30) as conn:
            row = conn.execute('SELECT COUNT(*) FROM restaurants').fetchone()
            return row[0] if row is not None else 0

    def _take_restaurant(self):
        with sqlite3.connect(self.db_names['status'], timeout=30, isolation_level='EXCLUSIVE') as conn:
            cursor = conn.cursor()
            cursor.execute('BEGIN EXCLUSIVE')
            row = cursor.execute('SELECT id FROM restaurants WHERE fetch_status = 0 LIMIT 1').fetchone()
            if row is not None:
                cursor.execute('UPDATE restaurants SET fetch_status = 1 WHERE id = ?', row)
            conn.commit()
            return row

    def _finish_restaurant(self, restaurant_id, status_code=2):
        with sqlite3.connect(self.db_names['status'], timeout=30) as conn:
            cursor = conn.cursor()
            cursor.execute(
                    '''UPDATE restaurants SET fetch_status = ?,commit_date = datetime('now','localtime') WHERE id = ?''',
                    (status_code, restaurant_id))
            conn.commit()
            row = cursor.execute('select count(*) from restaurants where fetch_status != 0 and fetch_status != 1').fetchone()
            if row is not None:
                self.num_finished = row[0]
        self._refresh_output()

    def _calculate_price(self, specfoods_json):
        price = 0
        for f_json in specfoods_json:
            price += f_json['price']
        return price

    def _store_menus(self, restaurant_id, menus):
        with sqlite3.connect(self.db_names['data']) as conn:
            cursor = conn.cursor()
            menus_json = json.loads(menus)

            for menu_category_json in menus_json:  # 分类
                for food_json in menu_category_json['foods']:
                    cursor.execute('''
                        INSERT INTO menus(restaurant_id,name,pinyin_name,rating,rating_count,price,month_sales,description,category_id,specfoods_json)
                        VALUES(?,?,?,?,?,?,?,?,?,?)
                    ''', (
                        food_json['restaurant_id'],
                        food_json['name'],
                        food_json['pinyin_name'],
                        food_json['rating'],
                        food_json['rating_count'],
                        self._calculate_price(food_json['specfoods']),
                        food_json['month_sales'],
                        food_json['description'],
                        food_json['category_id'],
                        str(food_json['specfoods'])
                    ))

            row = cursor.execute('SELECT COUNT(*) FROM menus').fetchone()
            if row is not None:
                self.num_menus = row[0]
            conn.commit()

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
            restaurant_id = self._take_restaurant()


def _fetch_menu_threading_worker(db_names):
    # print('线程%s已启动' % threading.current_thread().name)
    MenuFetcher(db_names).run()


class MenuFetchThreading(object):
    def __init__(self, db_names, num_threading=8):
        self.db_names = db_names
        self.num_threading = num_threading

    def run(self):
        threads = []
        for n in range(0, self.num_threading):
            threads.append(threading.Thread(target=_fetch_menu_threading_worker, args=(self.db_names,)))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()


def _fetch_menu_processing_worker(db_names):
    print('进程%d已启动' % os.getpid())
    MenuFetchThreading(db_names).run()


class MenuFetchProcessing(object):
    def __init__(self, db_names, num_processing=2):
        self.db_names = db_names
        self.num_processing = num_processing

    def run(self):
        processes = []
        for n in range(0, self.num_processing):
            processes.append(multiprocessing.Process(target=_fetch_menu_processing_worker, args=(self.db_names,)))

        print('开始抓取菜单数据...')
        for processor in processes:
            processor.start()

        for processor in processes:
            processor.join()

        print('\n抓取菜单数据完成')
