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


def _format_url_fields():
    fields = []
    for item in _FETCH_RESTAURANTS_REQUEST_ITEMS:
        fields.append('fields%5B%5D={}&'.format(item))
    return ''.join(fields)


_FETCH_RESTAURANTS_PREDEFINED_ITEMS = [
    {'key': 'extras%5B%5D', 'value': 'food_activity'},
    {'key': 'extras%5B%5D', 'value': 'restaurant_activity'},
    {'key': 'extras%5B%5D', 'value': 'certification'},
    {'key': 'offset', 'value': '0'},
    {'key': 'limit', 'value': '1000'},
    {'key': 'type', 'value': 'geohash'},
]


def _format_predefined_items():
    items = []
    for item in _FETCH_RESTAURANTS_PREDEFINED_ITEMS:
        items.append('{}={}&'.format(item['key'], item['value']))
    return ''.join(items)


_HOST = 'http://www.ele.me/restapi/v4/restaurants?'

_FETCH_RESTAURANTS_URL = _HOST + _format_url_fields() + _format_predefined_items() + \
                         'geohash={}&restaurant_category_id={}'


def create_url(geohash, category_id):
    return _FETCH_RESTAURANTS_URL.format(geohash, category_id)
