import sqlite3
import datetime
import geohash
import sys
from itertools import *


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


def _create_grid_table(conn, central, depth):
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
            http_code SMALLINT DEFAULT 0,
            commit_date DATETIME
            );
    ''')

    grid_iter = _MapGridIterator(central, depth)

    cursor.executemany('''INSERT INTO grid(geohash) VALUES (?);''', grid_iter)
    conn.commit()


def create_database(central, depth):
    db_name = datetime.datetime.now().strftime("%Y-%m-%d.db")
    print('初始化数据库:"{}"...'.format(db_name))
    conn = sqlite3.connect(db_name, isolation_level='EXCLUSIVE')
    _create_grid_table(conn, central, depth)
    conn.close()
    print('\n数据库初始化完成')

    return db_name
