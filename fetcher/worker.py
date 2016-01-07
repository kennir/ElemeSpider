import sqlite3

import requests

from fetcher import url_utils

_REQUEST_TIMEOUT = 3

# 207 全部快餐类
# 220 全部正餐类
# 233 小吃零食
# 239 甜品饮品
# 248 蛋糕
_SHOP_CATEGORIES = {207: [208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219],
                    220: [221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232],
                    233: [234, 235, 236, 237, 238],
                    239: [240, 241, 242, 243],
                    248: [249, 250]}


class Worker(object):
    def __init__(self, db_name):
        self.db_name = db_name

    def _take_geohash(self):
        with sqlite3.connect(self.db_name, isolation_level='EXCLUSIVE') as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT geohash FROM grid WHERE fetch_status = 0 LIMIT 1')
            geohash = cursor.fetchone()
            if geohash is not None:
                cursor.execute('UPDATE grid SET fetch_status = 1 WHERE geohash = ?', geohash)
            conn.commit()
            return geohash

    def _finish_geohash(self, geohash):
        with sqlite3.connect(self.db_name, isolation_level='EXCLUSIVE') as conn:
            conn = sqlite3.connect(self.db_name, isolation_level='EXCLUSIVE')
            conn.execute('UPDATE grid SET fetch_status = 2 WHERE geohash = ?', (geohash,))
            conn.commit()

    def _store_restaurant(self, geohash, major_cat, minor_cat, data):
        print(data)

    def _fetch_cell_catagory(self, geohash, major_cat, minor_cat):
        while True:
            r = requests.get(url_utils.create_url(geohash, minor_cat), _REQUEST_TIMEOUT)
            if r.status_code == requests.codes.ok:
                self._store_restaurant(geohash, major_cat, minor_cat, r.text)
                break

    def _fetch_cell(self, geohash):
        for major, minors in _SHOP_CATEGORIES.items():
            for minor in minors:
                self._fetch_cell_catagory(geohash, major, minor)
        self._finish_geohash(geohash)

    def run(self):
        while True:
            geohash = self._take_geohash()
            if geohash is None:
                break

            self._fetch_cell(geohash[0])
