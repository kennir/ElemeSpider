from math import *

import pandas as pd
import sqlalchemy
from pandas import ExcelWriter

_ORDER_BY_KEYWORD = ['rating_count', 'month_sales', 'revenue']

_COLUMN_NAME_DICT = {
    'rating_count': '点评数',
    'month_sales': '月销量',
    'revenue': '营业额'
}

_DISK_CATEGORY_KEYWORDS = {
    'staple': ['面', '饭', '粥', '馒头', '花卷', '馄饨', '饺', '包', '粉', '饼'],
    'drinking': ['酒,''咖啡', '雪碧', '可乐', '茶', '拿铁'],
    'dessert': ['布丁', '蛋糕', '饼干', '曲奇']
}

# 价格范围表
_PRICE_RANGES = [{'low': 1.0, 'high': 30.0},
                 {'low': 31.0, 'high': 50.0},
                 {'low': 51.0, 'high': 80.0},
                 {'low': 81.0, 'high': 120.0},
                 {'low': 121.0, 'high': 99999.0}]

# 用哪个值作为平均价格 [ mean_price, average_price ]
_AVERAGE_PRICE = 'average_price'


class Analyzer(object):
    is_limit_range = False

    # input Lat_A 纬度A
    # input Lng_A 经度A
    # input Lat_B 纬度B
    # input Lng_B 经度B
    # output distance 距离(km)
    def calcDistance(Lat_A, Lng_A, Lat_B, Lng_B):
        ra = 6378.140  # 赤道半径 (km)
        rb = 6356.755  # 极半径 (km)
        flatten = (ra - rb) / ra  # 地球扁率
        rad_lat_A = radians(Lat_A)
        rad_lng_A = radians(Lng_A)
        rad_lat_B = radians(Lat_B)
        rad_lng_B = radians(Lng_B)
        pA = atan(rb / ra * tan(rad_lat_A))
        pB = atan(rb / ra * tan(rad_lat_B))
        xx = acos(sin(pA) * sin(pB) + cos(pA) * cos(pB) * cos(rad_lng_A - rad_lng_B))
        c1 = (sin(xx) - xx) * (sin(pA) + sin(pB)) ** 2 / cos(xx / 2) ** 2
        c2 = (sin(xx) + xx) * (sin(pA) - sin(pB)) ** 2 / sin(xx / 2) ** 2
        dr = flatten / 8 * (c1 - c2)
        distance = ra * (xx + dr)
        return distance

    def __init__(self, db_name, lon=None, lat=None, range=None):
        self.db_name = db_name
        self.db_file = db_name + '-data.db'
        self.order_by = 'rating_count'
        self.ranking_list_size = 10
        self.restaurant_list_size = 150
        self.menu_list_size = 150
        self.scaling = 0.1

        print('加载数据库', self.db_file, '...')
        print('----------------------------------------------')
        engine = sqlalchemy.create_engine('sqlite:///' + self.db_file)
        self.restaurants_db = pd.read_sql_table('restaurants', engine)
        print('商家数(独立):\t', self.restaurants_db.shape[0])
        self.menus_db = pd.read_sql_table('menus', engine)
        print('菜单数:\t\t', self.menus_db.shape[0])
        category_db = pd.read_sql_table('category', engine)
        restaurant_categories_db = pd.read_sql_table('restaurant_categories', engine)
        print('商家数(分类):\t', restaurant_categories_db.shape[0])
        print('----------------------------------------------')

        if lon is not None and lat is not None and range is not None:
            print("排除范围外的商家")
            self.restaurants_db = self.restaurants_db[self.restaurants_db.apply(
                lambda x: Analyzer.calcDistance(x['latitude'], x['longitude'], lat, lon) <= range, axis=1)]
            print("排除后商家数(独立):\t", self.restaurants_db.shape[0])

        print('丢弃菜单重复数据')
        print('丢弃前菜单数量:\t', self.menus_db.shape[0])
        self.menus_db = self.menus_db.drop_duplicates(['name', 'restaurant_id'])
        print('丢弃后菜单数量:\t', self.menus_db.shape[0])

        self.menus_db = self.menus_db[self.menus_db['restaurant_id'].isin(self.restaurants_db['id']) != False]
        print('范围内饭店的菜单数量:\t', self.menus_db.shape[0])

        print('计算营业额...')
        self.menus_db['revenue'] = self.menus_db['price'] * self.menus_db['month_sales']

        print('合并营业额...')
        revenue_db = self.menus_db.loc[:, ['restaurant_id', 'revenue']].groupby(
            'restaurant_id').sum().reset_index(drop=False)
        self.restaurants_db = pd.merge(self.restaurants_db, revenue_db, left_on='id', right_on='restaurant_id',
                                       how='left')
        print('计算菜单平均价...')
        mean_db = self.menus_db.loc[:, ['restaurant_id', 'price']].groupby('restaurant_id').mean().reset_index(
            drop=False).rename(columns={'price': 'mean_price'})
        self.restaurants_db = pd.merge(self.restaurants_db, mean_db, on='restaurant_id')

        print('计算平均价格...')
        self.restaurants_db['average_price'] = self.restaurants_db['revenue'] / self.restaurants_db['month_sales']

        self.restaurants_db['revenue'] = self.restaurants_db['revenue'].fillna(0)
        del self.restaurants_db['restaurant_id']

        self.num_restaurants = self.restaurants_db.shape[0]
        self.total_revenue = self.restaurants_db['revenue'].sum()
        self.total_sales = self.restaurants_db['month_sales'].sum()

        print('合并商家类型...')
        self.restaurants_db = pd.merge(self.restaurants_db, restaurant_categories_db, left_on='id',
                                       right_on='restaurant_id', how='right')
        del self.restaurants_db['restaurant_id']

        print('合并类型详细信息...')
        category_db = category_db.rename(columns={'id': 'cat_id', 'name': 'cat_name'})
        self.restaurants_db = pd.merge(self.restaurants_db, category_db, left_on='category_id', right_on='cat_id',
                                       how='left')
        print('为菜单生成种类分类信息...')
        self.menus_db['type'] = [self._determine_dish_type(n) for n in self.menus_db['name']]

        print('为菜单生成商铺分类信息...')
        category_db = self.restaurants_db.loc[:, ['id', 'cat_name']].rename(columns={'id': 'restaurant_id'})
        self.menus_db = pd.merge(self.menus_db, category_db, on='restaurant_id')

    @staticmethod
    def _determine_dish_type(name):
        for type, keywords in _DISK_CATEGORY_KEYWORDS.items():
            for keyword in keywords:
                if keyword in name:
                    return type
        return 'vegetable'

    @staticmethod
    def _check_row_count(df, needed):
        count = df.shape[0]
        if count < needed:
            new_df = pd.DataFrame(index=range(needed - count), columns=df.columns)
            new_df = pd.concat([df, new_df], ignore_index=True)
            return new_df
        else:
            return df

    @staticmethod
    def _merge_dishes(df, order_by):
        output_df = df.loc[:, ['name', order_by]]
        output_df = output_df.groupby('name').sum().sort_values(by=order_by, ascending=False).reset_index(drop=False)
        return output_df

    def _generate_restaurant_ranking_by_categories(self, category_df, restaurants_db):
        def _generate_by_category(df, cat_name, order_by, ranking_list_size):
            column_name_map = {'name': '2.1 店铺名', 'rating_count': '2.2 点评数', 'month_sales': '2.3 月销量',
                               'revenue': '2.4 营业额'}

            df = df[df['cat_name'] == cat_name].loc[:,
                 ['name', 'rating_count', 'month_sales', 'revenue']].sort_values(by=order_by, ascending=False).iloc[
                 0:ranking_list_size].rename(columns=column_name_map).reset_index(drop=True)
            df = self._check_row_count(df, self.ranking_list_size)
            return df

        df = pd.DataFrame()
        for cat_name in category_df['1.0 菜系品类']:
            ranking_df = _generate_by_category(restaurants_db, cat_name, self.order_by, self.ranking_list_size)
            df = pd.concat([df, ranking_df], ignore_index=True)
        df.reset_index(drop=True)
        return df

    def _generate_menu_ranking_by_categories(self, category_df, menus_db, dish_type, columns):
        """
        生成菜单的排行榜
        :param category_df: 商家的分类
        :param dish_type: 菜品的类型
        :param columns: 列名
        :return: 菜单排行榜
        """

        def _generate_by_category(order_by, ranking_list_size):
            df = menus_db[(menus_db.cat_name == cat_name) & (menus_db.type == dish_type)]

            # 合并菜品
            df = self._merge_dishes(df, order_by).iloc[0:ranking_list_size].reset_index(drop=True)
            df = self._check_row_count(df, ranking_list_size)
            df.columns = columns
            return df

        df = pd.DataFrame()
        for cat_name in category_df['1.0 菜系品类']:
            ranking_df = _generate_by_category(self.order_by, self.ranking_list_size)
            df = pd.concat([df, ranking_df], ignore_index=True)
        df.reset_index(drop=True)
        return df

    def _generate_category_ranking(self, restaurants_db, size=None, expandable=True):
        """
        根据当前的排序生成根据商铺分类排行的总表
        :param size: 返回数量
        :param expandable: 是否将每一条纪录复制N条
        :return: 商铺的分类排行榜
        """
        df = restaurants_db.loc[:, ['cat_name', 'rating_count', 'month_sales', 'revenue']].groupby(
            'cat_name').sum().sort_values(by=self.order_by, ascending=False).reset_index(drop=False)
        df.columns = ['1.0 菜系品类', '1.1 点评数', '1.1 月销量', '1.1 营业额']

        if size is not None:
            df = df.iloc[0:size]

            if expandable is True:
                df = pd.concat([df] * size).sort_values(by='1.1 {}'.format(_COLUMN_NAME_DICT[self.order_by]),
                                                        ascending=False).reset_index(drop=True)
        return df

    def _generate_summary(self, restaurants_db):
        """
        创建预览
        :param restaurants_db:
        :return:
        """
        values = [self.num_restaurants, self.total_revenue, self.total_revenue / self.num_restaurants, self.total_sales]

        df = pd.DataFrame(values, index=['商家数', '总营业额', '平均营业额/商家', '总销量(未做缩放)'])
        return df

    def _generate_comprehensive_report(self, restaurants_db, menus_db, price_range=None):
        """
        创建综合的报告
        :return: 生成的报告DataFrame
        """

        restaurants_df = restaurants_db
        menus_df = menus_db
        if price_range is not None:
            restaurants_df = restaurants_df[(restaurants_df[_AVERAGE_PRICE] >= price_range['low']) & (
                restaurants_df[_AVERAGE_PRICE] <= price_range['high'])]
            menus_df = menus_df[(menus_df['price'] >= price_range['low']) & (menus_df['price'] <= price_range['high'])]

        df = self._generate_category_ranking(restaurants_df, size=self.ranking_list_size)

        cat_df = df.drop_duplicates('1.0 菜系品类')
        df = pd.concat([df, self._generate_restaurant_ranking_by_categories(cat_df, restaurants_df)], axis=1)

        # 分品类
        name = _COLUMN_NAME_DICT[self.order_by]
        dish_types = [
            {'cat': 'vegetable', 'col': ['3.0 菜', '3.1 {}'.format(name)]},
            {'cat': 'staple', 'col': ['4.0 主食', '4.1 {}'.format(name)]},
            {'cat': 'drinking', 'col': ['5.0 饮料', '5.1 {}'.format(name)]},
            {'cat': 'dessert', 'col': ['6.0 甜点', '6.1 {}'.format(name)]}
        ]
        for dish_type in dish_types:
            df = pd.concat(
                [df,
                 self._generate_menu_ranking_by_categories(cat_df, menus_df, dish_type['cat'], dish_type['col'])],
                axis=1)
        return df

    def _generate_restaurant_report(self, restaurant_db):
        print('生成商家报告...')

        def generate_restaurant_ranking(restaurant_db, order_by, restaurant_list_size):
            df = restaurant_db.loc[:,
                 ['name', 'rating_count', 'month_sales', 'revenue', _AVERAGE_PRICE]].sort_values(by=order_by,
                                                                                                 ascending=False).drop_duplicates(
                subset='name').iloc[
                 0:restaurant_list_size].reset_index(drop=True)

            df = df.reindex_axis(['name', 'rating_count', 'month_sales', 'revenue', _AVERAGE_PRICE], axis=1)
            return df

        df = generate_restaurant_ranking(restaurant_db, self.order_by, self.restaurant_list_size)

        for pr in _PRICE_RANGES:
            # print('生成商家报告({}-{})...'.format(pr['low'], pr['high']))
            filter_df = restaurant_db[
                (restaurant_db[_AVERAGE_PRICE] >= pr['low']) & (restaurant_db[_AVERAGE_PRICE] <= pr['high'])]
            df = pd.concat([df, generate_restaurant_ranking(filter_df, self.order_by, self.restaurant_list_size)],
                           axis=1)
        df.columns = ['店铺名', '点评数', '销量', '营业额', '平均售价',
                      '店铺名(<30)', '点评数(<30)', '销量(<30)', '营业额(<30)', '平均售价(<30)',
                      '店铺名(31-50)', '点评数(31-50)', '销量(31-50)', '营业额(<31-50)', '平均售价(<31-50)',
                      '店铺名(51-80)', '点评数(51-80)', '销量(51-80)', '营业额(<51-80)', '平均售价(<51-80)',
                      '店铺名(81-120)', '点评数(81-120)', '销量(81-120)', '营业额(<81-120)', '平均售价(<81-120)',
                      '店铺名(>120)', '点评数(>120)', '销量(>120)', '营业额(>120)', '平均售价(>120)']
        return df

    def _generate_menu_report(self, menus_db):
        print('生成菜品报告...')

        f = {'rating_count': 'sum', 'month_sales': 'sum', 'price': 'mean', 'revenue': 'sum'}
        menus_df = menus_db.loc[:, ['name', 'rating_count', 'month_sales', 'price', 'revenue']].groupby('name').agg(
            f).reindex_axis(['rating_count', 'month_sales', 'price', 'revenue'], axis=1)

        def generate_menu_ranking(menu_df, order_by, menu_list_size):
            output_df = menu_df.sort_values(by=order_by, ascending=False).iloc[0:menu_list_size].reset_index(drop=False)
            output_df = self._check_row_count(output_df, menu_list_size)
            return output_df

        df = generate_menu_ranking(menus_df, self.order_by, self.menu_list_size)
        del df['revenue']

        for pr in _PRICE_RANGES:
            # print('生成菜品报告({}-{})...'.format(pr['low'], pr['high']))
            dest_df = menus_df[(menus_df['price'] >= pr['low']) & (menus_df['price'] <= pr['high'])]
            dest_df = generate_menu_ranking(dest_df, self.order_by, self.menu_list_size)
            del dest_df['revenue']
            df = pd.concat([df, dest_df], axis=1)

        df.columns = ['推荐菜', '点评数', '销量', '价格',
                      '推荐菜(<30)', '点评数(<30)', '销量(<30)', '价格(<30)',
                      '推荐菜(31-50)', '点评数(31-50)', '销量(31-50)', '价格(31-50)',
                      '推荐菜(51-80)', '点评数(51-80)', '销量(51-80)', '价格(51-80)',
                      '推荐菜(81-120)', '点评数(81-120)', '销量(81-120)', '价格(81-120)',
                      '推荐菜(>120)', '点评数(>120)', '销量(>120)', '价格(>120)']
        return df

    def _generate_restaurant_distribution(self, restaurant_db):
        print('生成商家分布报告...')

        order_by_name = _COLUMN_NAME_DICT[self.order_by]
        columns = [{'cnt': '2.0 店铺数', 'sum': '2.1 {}'.format(order_by_name), 'avg': '2.1 平均'},
                   {'cnt': '3.0 店铺数(<30)', 'sum': '3.1 {}(<30)'.format(order_by_name), 'avg': '3.1 平均(<30)'},
                   {'cnt': '4.0 店铺数(31-50)', 'sum': '4.1 {}(31-50)'.format(order_by_name), 'avg': '4.1 平均(31-50)'},
                   {'cnt': '5.0 店铺数(51-80)', 'sum': '5.1 {}(51-80)'.format(order_by_name), 'avg': '5.1 平均(51-80)'},
                   {'cnt': '6.0 店铺数(81-120)', 'sum': '6.1 {}(81-120)'.format(order_by_name), 'avg': '6.1 平均(81-120)'},
                   {'cnt': '7.0 店铺数(>120)', 'sum': '7.1 {}(>120)'.format(order_by_name), 'avg': '7.1 平均(>120)'}]

        def count_num_restaurants(cat_name, input_df, order_by, column):
            restaurant_df = input_df[input_df.cat_name == cat_name]

            num_restaurants = restaurant_df.shape[0]
            sum_value = restaurant_df[order_by].sum()
            average_value = (sum_value / num_restaurants) if num_restaurants != 0 else 0

            output_df = pd.DataFrame({
                column['cnt']: num_restaurants,
                column['sum']: sum_value,
                column['avg']: average_value,
            }, index=[0])
            output_df = output_df.reindex_axis([column['cnt'], column['sum'], column['avg']], axis=1)
            return output_df

        def generate_distribution_by_category(input_df, order_by, column):
            output_df = pd.DataFrame()
            for idx, row in category_df.iterrows():
                sum_df = count_num_restaurants(row['1.0 菜系品类'], input_df, order_by, column)
                output_df = pd.concat([output_df, sum_df], ignore_index=True)
            return output_df

        category_df = self._generate_category_ranking(restaurant_db, size=self.ranking_list_size, expandable=False)
        dist = generate_distribution_by_category(restaurant_db, self.order_by, columns[0])
        dist = pd.concat([category_df, dist], axis=1)

        column_index = 1
        for pr in _PRICE_RANGES:
            # print('生成商家分布报告({}-{})...'.format(pr['low'], pr['high']))
            df = restaurant_db[
                (restaurant_db[_AVERAGE_PRICE] >= pr['low']) & (restaurant_db[_AVERAGE_PRICE] <= pr['high'])]
            df = generate_distribution_by_category(df, self.order_by, columns[column_index])
            column_index += 1
            dist = pd.concat([dist, df], axis=1)

        return dist

    def _create_excel(self, excel_filename):
        """
        生成EXCEL文件
        :param excel_filename: EXCEL文件名
        :return: None
        """
        sheet_names = ['Summary', '汇总', '<30', '31 - 50', '51 = 80', '81 - 120', '>121', '商家', '菜单', '分布']

        reports = []
        print('----------------------------------------------')
        print('生成Excel:\t', excel_filename)
        print('生成分类总榜...')
        reports.append(self._generate_summary(self.restaurants_db))
        reports.append(self._generate_comprehensive_report(self.restaurants_db, self.menus_db))

        # 生成所有的价格分榜单
        for price_range in _PRICE_RANGES:
            # print('生成分类榜({}-{})...'.format(price_range['low'], price_range['high']))
            reports.append(self._generate_comprehensive_report(self.restaurants_db, self.menus_db, price_range))

        reports.append(self._generate_restaurant_report(self.restaurants_db))
        reports.append(self._generate_menu_report(self.menus_db))
        reports.append(self._generate_restaurant_distribution(self.restaurants_db))

        with ExcelWriter(excel_filename) as writer:
            for idx in range(len(reports)):
                reports[idx].to_excel(writer, sheet_names[idx])

    def _scale(self):
        """
        减少特定种类的饭店的数值
        """
        print('对特定种类(麻辣烫,香锅,烧烤)的商店缩放数据...')

        categories = ['麻辣烫', '香锅', '烧烤']
        columns = ['month_sales', 'rating_count']
        for cat in categories:
            for col in columns:
                self.restaurants_db.loc[self.restaurants_db.cat_name == cat, col] = self.restaurants_db.loc[
                                                                                        self.restaurants_db.cat_name == cat, col] * self.scaling

    def generate(self):
        for order_by in _ORDER_BY_KEYWORD:
            self.order_by = order_by
            self._create_excel('{}-{}.xlsx'.format(self.db_name, _COLUMN_NAME_DICT[self.order_by]))

        print('=====================================')
        self._scale()

        for order_by in _ORDER_BY_KEYWORD:
            self.order_by = order_by
            self._create_excel('{}-{}-缩放.xlsx'.format(self.db_name, _COLUMN_NAME_DICT[self.order_by]))
