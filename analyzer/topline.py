import pandas as pd
import sqlalchemy

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
_AVENAGE_PRICE = 'average_price'


class Analyzer(object):
    def __init__(self, db_name):
        self.db_name = db_name
        self.order_by = 'rating_count'
        self.ranking_list_size = 10

        print('加载数据库', db_name, '...')
        print('----------------------------------------------')
        engine = sqlalchemy.create_engine('sqlite:///' + db_name)
        self.restaurants_db = pd.read_sql_table('restaurants', engine)
        print('商家数(独立):\t', self.restaurants_db.shape[0])
        self.menus_db = pd.read_sql_table('menus', engine)
        print('菜单数:\t\t', self.menus_db.shape[0])
        category_db = pd.read_sql_table('category', engine)
        restaurant_categories_db = pd.read_sql_table('restaurant_categories', engine)
        print('商家数(分类):\t', restaurant_categories_db.shape[0])
        print('----------------------------------------------')

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

    def _generate_comprehensive_report(self, restaurants_db, menus_db, price_range=None):
        """
        创建综合的报告
        :return: 生成的报告DataFrame
        """

        restaurants_df = restaurants_db
        menus_df = menus_db
        if price_range is not None:
            restaurants_df = restaurants_df[(restaurants_df[_AVENAGE_PRICE] >= price_range['low']) & (
                restaurants_df[_AVENAGE_PRICE] <= price_range['high'])]
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

    def _create_excel(self, excel_filename):
        """
        生成EXCEL文件
        :param excel_filename: EXCEL文件名
        :return: None
        """
        sheet_names = ['汇总', '<30', '31 - 50', '51 = 80', '81 - 120', '>121', '商家', '菜单', 'Dist']

        reports = []

        print('\n\n生成Excel:\t', excel_filename)
        print('----------------------------------------------')
        print('生成分类总榜...')
        reports.append(self._generate_comprehensive_report(self.restaurants_db, self.menus_db))

        # 生成所有的价格分榜单
        for price_range in _PRICE_RANGES:
            print('生成分类榜({}-{})...'.format(price_range['low'], price_range['high']))
            reports.append(self._generate_comprehensive_report(self.restaurants_db, self.menus_db, price_range))

    def generate(self, order_by):
        self.order_by = order_by
        self._create_excel('top({}).xlsx'.format(order_by))
