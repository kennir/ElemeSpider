import sqlite3
import datetime
import geohash
import sys
from itertools import *


class _GeohashGenerator(object):
    '''
    Attributes:
        max_depth: max depth
        current_depth: current depth
    '''

    def __init__(self, central_geohash, depth=65):
        self._cells = set()
        self._next_batch = set([central_geohash])
        self._computed_cells = set()
        self.max_depth = depth
        self.current_depth = 0
        self._refresh_output()


    def _add_neighbors(self, cell):
        if cell in self._computed_cells:
            return

        n = geohash.neighbors(cell)

        cond = lambda c: (c in self._computed_cells) or (c in self._cells)
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
        sys.stdout.write("\r")
        sys.stdout.write("Generating geohash: % 2d / % 2d" % (self.current_depth, self.max_depth))
        sys.stdout.flush()

    def next_cell(self):
        if len(self._cells) == 0 and self.current_depth < self.max_depth:
            self._advance_depth()

        return self._take_cell()


_SQL_CREATE_GRID_STATUS_TABLE = '''CREATE TABLE grid_status
        (HASH TEXT PRIMARY KEY NOT NULL,
        FETCHED BOOLEAN DEFAULT 0,
        DATE DATETIME);'''

_SQL_DROP_GRID_STATUS_TABLE = '''DROP TABLE IF EXISTS grid_status'''


_SQL_INSERT_INTO_GRID_STATUS = '''INSERT INTO grid_status VALUES ('{}',0,NULL);'''


class DbConn(object):
    '''
    Attributies:
        db_name: database file name
    '''

    def __init__(self, central, depth, db_name=None):
        self.db_name = db_name if db_name is not None else datetime.datetime.now().strftime("%Y-%m-%d.db")
        self._conn = sqlite3.connect(self.db_name)
        if db_name is None:
            self._init_new_database(central, depth)

    def _init_grid_table(self, central, depth):
        '''
        Create geohash-grid table
        '''
        self._conn.execute(_SQL_DROP_GRID_STATUS_TABLE)
        self._conn.execute(_SQL_CREATE_GRID_STATUS_TABLE)

        print('Preparing table for grid')
        hash = _GeohashGenerator(central, depth)

        while True:
            cell = hash.next_cell()
            if cell is None:
                break

            self._conn.execute(_SQL_INSERT_INTO_GRID_STATUS.format(cell))
        self._conn.commit()
        print('\nGrid OK!')

    def _init_new_database(self, central, depth):
        self._init_grid_table(central, depth)
