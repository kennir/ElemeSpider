import json
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

    def _log_fetch_restaurant_error(self, geohash, http_code, error_msg):
        conn = sqlite3.connect(self.db_names['log'])
        conn.execute('INSERT INTO fetch_restaurant_log VALUES(?,?,?)',
                              (geohash, http_code, error_msg))
        conn.commit()
        conn.close()

    def _take_geohash(self):
        conn = sqlite3.connect(self.db_names['grid'], timeout=30, isolation_level='EXCLUSIVE')
        cursor = conn.cursor()
        cursor.execute('SELECT geohash FROM grid WHERE fetch_status = 0 LIMIT 1')
        geohash = cursor.fetchone()
        if geohash is not None:
            cursor.execute('UPDATE grid SET fetch_status = 1 WHERE geohash = ?', geohash)
        conn.commit()
        conn.close()
        return geohash

    def _refresh_output(self):
        sys.stdout.write("\r抓取地图网格商家数据(%d/%d) %.2f%% 商家数:%d" %
                         (self.num_finished, self.num_cells,
                          self.num_finished / self.num_cells * 100.0,
                          self.num_restaurants))
        sys.stdout.flush()

    def _finish_geohash(self, geohash):
        conn = sqlite3.connect(self.db_names['grid'], timeout=30, isolation_level='EXCLUSIVE')
        conn.execute('UPDATE grid SET fetch_status = 2 WHERE geohash = ?', (geohash,))
        conn.commit()
        conn.close()
        self.num_finished += 1
        self._refresh_output()

    def _store_restaurants(self, geohash, minor_cat, restaurants):
        conn = sqlite3.connect(self.db_names['data'])
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

            cursor.execute('SELECT COUNT(*) FROM restaurants')
            result = cursor.fetchone()
            if result is not None:
                self.num_restaurants = result[0]
        conn.commit()
        conn.close()

    def _fetch_cell_category(self, geohash, minor_cat):
        while True:
            r = requests.get(url_utils.create_url(geohash, minor_cat), _REQUEST_TIMEOUT)
            if r.status_code == requests.codes.ok:
                self._store_restaurants(geohash, minor_cat, r.text)
                break
            else:
                self._log_fetch_restaurant_error(geohash, r.status_code, r.text)

    def _num_cells(self):
        num = 0
        conn = sqlite3.connect(self.db_names['grid'], timeout=30)
        cursor = conn.execute('SELECT COUNT(*) FROM grid')
        result = cursor.fetchone()
        if result is not None:
            num = result[0]
        return num

    def _fetch_cell(self, geohash):
        for major, minors in RESTAURANT_CATEGORIES.items():
            for minor in minors:
                self._fetch_cell_category(geohash, minor)
        self._finish_geohash(geohash)

    def run(self):
        while True:
            geohash = self._take_geohash()
            if geohash is None:
                break

            self._fetch_cell(geohash[0])


def _fetch_threading_worker(db_names):
    RestaurantFetcher(db_names).run()


class RestaurantFetchThreading(object):
    def __init__(self, db_names, num_threading=8):
        self.db_names = db_names
        self.num_threading = num_threading

    def run(self):
        threads = []
        for n in range(0, self.num_threading):
            threads.append(threading.Thread(target=_fetch_threading_worker, args=(self.db_names,)))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()


def _fetch_processing_worker(db_names):
    RestaurantFetchThreading(db_names).run()


class RestaurantFetchProcessing(object):
    def __init__(self, db_names, num_processing=4):
        self.db_names = db_names
        self.num_processing = num_processing

    def run(self):
        processes = []
        for n in range(0, self.num_processing):
            processes.append(threading.Thread(target=_fetch_processing_worker, args=(self.db_names,)))

        for processor in processes:
            processor.start()

        for processor in processes:
            processor.join()
