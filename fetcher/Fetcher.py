import requests
import sqlite3

_FETCH_RESTAURANTS_REQUEST_ITEMS = ['id',
                                    'name',
                                    'phone',
                                    'name_for_url',
                                    'flavors', 'rating',
                                    'is_free_delivery',
                                    'delivery_fee',
                                    'minimum_order_amount',
                                    'rating_count',
                                    'month_sales',
                                    'minimum_free_delivery_amount&',
                                    'promotion_info',
                                    'address',
                                    'delivery_fee',
                                    'order_lead_time',
                                    'latitude',
                                    'longitude']

_FETCH_RESTAURANTS_PREDEFINED_ITEMS = [
    {'extras%5B%5D', 'food_activity'},
    {'extras%5B%5D', 'restaurant_activity'},
    {'extras%5B%5D', 'certification'},
    {'offset', '24'},
    {'limit', '1000'},
    {'type', 'geohash'},
]

# 207 全部快餐类
# 220 全部正餐类
# 233 小吃零食
# 239 甜品饮品
# 248 蛋糕
shop_categories = {207: [208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219],
                   220: [221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232],
                   233: [234, 235, 236, 237, 238],
                   239: [240, 241, 242, 243],
                   248: [249, 250]}

_FETCH_RESTAURANTS_URL = 'http://www.ele.me/restapi/v4/restaurants?{}{}geohash={}&restaurant_category_id={}'


class _FetchTask(object):
    def __init__(self):
        self.geohash = None
        pass

    def fetch(self, geohash):
        self.geohash = geohash


class Fetcher(object):
    """
    Attributes:
        db_name: database name
    """

    def __init__(self, db_name):
        self.db_name = db_name
        self._conn = sqlite3.connect(db_name)
