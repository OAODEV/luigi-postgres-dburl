# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import unittest

import luigi
import luigi_postgres_dburl
import testing.postgresql
from psycopg2.extensions import make_dsn
from mock import patch, PropertyMock

"""
Typical use cases that should be tested:

* Daily overwrite of all data in table
* Daily inserts of new segment in table
* (Daily insertion/creation of new table)
* Daily insertion of multiple (different) new segments into table


"""
Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)


# to avoid copying:

class CopyToTestDB(luigi_postgres_dburl.CopyToTable):

    # Use this as a placeholder before it gets overridden in each of the tests
    dsn = "postgres://user@localhost/dbname"


class TestPostgresTask(CopyToTestDB):
    table = 'test_table'
    columns = (('test_text', 'text'),
               ('test_int', 'int'),
               ('test_float', 'float'))

    def create_table(self, connection):
        connection.cursor().execute(
            "CREATE TABLE {table} (id SERIAL PRIMARY KEY, test_text TEXT, test_int INT, test_float FLOAT)"
            .format(table=self.table))

    def rows(self):
        yield 'foo', 123, 123.45
        yield None, '-100', '5143.213'
        yield '\t\n\r\\N', 0, 0
        yield u'éцү我', 0, 0
        yield '', 0, r'\N'  # Test working default null character


class MetricBase(CopyToTestDB):
    table = 'metrics'
    columns = [('metric', 'text'),
               ('value', 'int')
               ]


class Metric1(MetricBase):
    param = luigi.Parameter()

    def rows(self):
        yield 'metric1', 1
        yield 'metric1', 2
        yield 'metric1', 3


class Metric2(MetricBase):
    param = luigi.Parameter()

    def rows(self):
        yield 'metric2', 1
        yield 'metric2', 4
        yield 'metric2', 3


class TestPostgresImportTask(unittest.TestCase):

    def setUp(self):
        # Use the generated Postgresql class instead of testing.postgresql.Postgresql
        self.postgresql = testing.postgresql.Postgresql()

    def tearDown(self):
        self.postgresql.stop()

    def test_default_escape(self):
        self.assertEqual(luigi_postgres_dburl.default_escape('foo'), 'foo')
        self.assertEqual(luigi_postgres_dburl.default_escape('\n'), '\\n')
        self.assertEqual(luigi_postgres_dburl.default_escape('\\\n'), '\\\\\\n')
        self.assertEqual(luigi_postgres_dburl.default_escape('\n\r\\\t\\N\\'),
                         '\\n\\r\\\\\\t\\\\N\\\\')

    def test_repeat(self):
        with patch.object(CopyToTestDB, 'dsn', new_callable=PropertyMock) as mock:
            mock.return_value = make_dsn(**self.postgresql.dsn())
            task = TestPostgresTask()
            conn = task.output().connect()
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute('DROP TABLE IF EXISTS {table}'.format(table=task.table))
            cursor.execute('DROP TABLE IF EXISTS {marker_table}'.format(marker_table=luigi_postgres_dburl.PostgresTarget.marker_table))

            luigi.build([task], local_scheduler=True)
            luigi.build([task], local_scheduler=True)  # try to schedule twice

            cursor.execute("""SELECT test_text, test_int, test_float
                              FROM test_table
                              ORDER BY id ASC""")

            rows = tuple(cursor)

            self.assertEqual(rows, (
                ('foo', 123, 123.45),
                (None, -100, 5143.213),
                ('\t\n\r\\N', 0.0, 0),
                (u'éцү我', 0, 0),
                (u'', 0, None),  # Test working default null charcter
            ))

    def test_multimetric(self):
        with patch.object(CopyToTestDB, 'dsn', new_callable=PropertyMock) as mock:
            mock.return_value = make_dsn(**self.postgresql.dsn())
            metrics = MetricBase()
            conn = metrics.output().connect()
            conn.autocommit = True
            conn.cursor().execute('DROP TABLE IF EXISTS {table}'.format(table=metrics.table))
            conn.cursor().execute('DROP TABLE IF EXISTS {marker_table}'.format(marker_table=luigi_postgres_dburl.PostgresTarget.marker_table))
            luigi.build([Metric1(20), Metric1(21), Metric2("foo")], local_scheduler=True)

            cursor = conn.cursor()
            cursor.execute('select count(*) from {table}'.format(table=metrics.table))
            self.assertEqual(tuple(cursor), ((9,),))

    def test_clear(self):
        class Metric2Copy(Metric2):

            def init_copy(self, connection):
                query = "TRUNCATE {0}".format(self.table)
                connection.cursor().execute(query)

        with patch.object(CopyToTestDB, 'dsn', new_callable=PropertyMock) as mock:
            mock.return_value = make_dsn(**self.postgresql.dsn())
            clearer = Metric2Copy(21)
            conn = clearer.output().connect()
            conn.autocommit = True
            conn.cursor().execute('DROP TABLE IF EXISTS {table}'.format(table=clearer.table))
            conn.cursor().execute('DROP TABLE IF EXISTS {marker_table}'.format(marker_table=luigi_postgres_dburl.PostgresTarget.marker_table))

            luigi.build([Metric1(0), Metric1(1)], local_scheduler=True)
            luigi.build([clearer], local_scheduler=True)
            cursor = conn.cursor()
            cursor.execute('select count(*) from {table}'.format(table=clearer.table))
            self.assertEqual(tuple(cursor), ((3,),))
