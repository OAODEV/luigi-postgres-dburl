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
"""
Implements a subclass of :py:class:`~luigi.target.Target` that writes data to Postgres.
Also provides a helper task to copy data into a Postgres table or return data from a query.
"""

import abc
import datetime
import logging
import tempfile
from luigi_postgres_dburl.multi_replacer import MultiReplacer

from luigi import six

import luigi

logger = logging.getLogger('luigi-interface')

try:
    import psycopg2
    import psycopg2.errorcodes
except ImportError:
    logger.warning("Loading postgres module without psycopg2 installed. Will crash at runtime if postgres functionality is used.")


# these are the escape sequences recognized by postgres COPY
# according to http://www.postgresql.org/docs/8.1/static/sql-copy.html
default_escape = MultiReplacer([('\\', '\\\\'),
                                ('\t', '\\t'),
                                ('\n', '\\n'),
                                ('\r', '\\r'),
                                ('\v', '\\v'),
                                ('\b', '\\b'),
                                ('\f', '\\f')
                                ])


class PostgresTarget(luigi.Target):
    """
    Target for a resource in Postgres.

    This will rarely have to be directly instantiated by the user.
    """
    marker_table = luigi.configuration.get_config().get('postgres',
                                                        'marker-table',
                                                        'table_updates')

    # Use DB side timestamps or client side timestamps in the marker_table
    use_db_timestamps = True

    def __init__(
        self, table, update_id, dsn
    ):
        """
        Args:
            table (str): Table PostgresTarget interacts with
            update_id (str): An identifier for this data set
            dsn (str): postgres connection string

        """
        self.table = table
        self.update_id = update_id
        self.dsn = dsn

    def touch(self, connection=None):
        """
        Mark this update as complete.

        Important: If the marker table doesn't exist, the connection transaction will be aborted
        and the connection reset.
        Then the marker table will be created.
        """
        self.create_marker_table()

        if connection is None:
            # TODO: test this
            connection = self.connect()
            connection.autocommit = True  # if connection created here, we commit it here

        if self.use_db_timestamps:
            connection.cursor().execute(
                """INSERT INTO {marker_table} (update_id, target_table)
                   VALUES (%s, %s)
                """.format(marker_table=self.marker_table),
                (self.update_id, self.table))
        else:
            connection.cursor().execute(
                """INSERT INTO {marker_table} (update_id, target_table, inserted)
                         VALUES (%s, %s, %s);
                    """.format(marker_table=self.marker_table),
                (self.update_id, self.table,
                 datetime.datetime.now()))

        # make sure update is properly marked
        assert self.exists(connection)

    def exists(self, connection=None):
        if connection is None:
            connection = self.connect()
            connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute("""SELECT 1 FROM {marker_table}
                WHERE update_id = %s
                LIMIT 1""".format(marker_table=self.marker_table),
                           (self.update_id,)
                           )
            row = cursor.fetchone()
        except psycopg2.ProgrammingError as e:
            if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                row = None
            else:
                raise
        return row is not None

    def connect(self):
        """
        Get a psycopg2 connection object to the database where the table is.
        """
        connection = psycopg2.connect(
            dsn=self.dsn)
        connection.set_client_encoding('utf-8')
        return connection

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.

        Using a separate connection since the transaction might have to be reset.
        """
        connection = self.connect()
        connection.autocommit = True
        cursor = connection.cursor()
        if self.use_db_timestamps:
            sql = """ CREATE TABLE IF NOT EXISTS {marker_table} (
                      update_id TEXT PRIMARY KEY,
                      target_table TEXT,
                      inserted TIMESTAMP DEFAULT NOW())
                  """.format(marker_table=self.marker_table)
        else:
            sql = """ CREATE TABLE IF NOT EXISTS {marker_table} (
                      update_id TEXT PRIMARY KEY,
                      target_table TEXT,
                      inserted TIMESTAMP);
                  """.format(marker_table=self.marker_table)
        cursor.execute(sql)
        connection.close()

    def open(self, mode):
        raise NotImplementedError("Cannot open() PostgresTarget")


class CopyToTable(luigi.task.MixinNaiveBulkComplete, luigi.Task):
    """
    An abstract task for inserting a data set into RDBMS.

    Usage:

        Subclass and override the following attributes:

        * `table`
        * `columns`
        * `dsn`
    """

    @abc.abstractproperty
    def table(self):
        return None

    @abc.abstractproperty
    def dsn(self):
        """Example usage: return postgres://user@host:port/dbname'"""
        return None

    # specify the columns that are to be inserted (same as are returned by columns)
    # overload this in subclasses with the either column names of columns to import:
    # e.g. ['id', 'username', 'inserted']
    # or tuples with column name, postgres column type strings:
    # e.g. [('id', 'SERIAL PRIMARY KEY'), ('username', 'VARCHAR(255)'), ('inserted', 'DATETIME')]
    columns = []

    # options
    null_values = (None,)  # container of values that should be inserted as NULL values

    column_separator = "\t"  # how columns are separated in the file copied into postgres

    def init_copy(self, connection):
        """
        Override to perform custom queries.

        Any code here will be formed in the same transaction as the main copy, just prior to copying data.
        Example use cases include truncating the table or removing all data older than X in the database
        to keep a rolling window of data available in the table.
        """
        pass

    def post_copy(self, connection):
        """
        Override to perform custom queries.

        Any code here will be formed in the same transaction as the main copy, just after copying data.
        Example use cases include cleansing data in temp table prior to insertion into real table.
        """
        pass

    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.
        """
        with self.input().open('r') as fobj:
            for line in fobj:
                yield line.strip('\n').split('\t')

# everything below will rarely have to be overridden

    def output(self):
        """
        Returns a PostgresTarget representing the inserted dataset.

        Normally you don't override this.
        """
        return PostgresTarget(
            table=self.table,
            update_id=self.update_id,
            dsn=self.dsn
        )

    def copy(self, cursor, file):
        if isinstance(self.columns[0], six.string_types):
            column_names = self.columns
        elif len(self.columns[0]) == 2:
            column_names = [c[0] for c in self.columns]
        else:
            raise Exception('columns must consist of column strings or (column string, type string) tuples (was %r ...)' % (self.columns[0],))
        cursor.copy_from(file, self.table, null=r'\\N', sep=self.column_separator, columns=column_names)

    def create_table(self, connection):
        """
        Override to provide code for creating the target table.

        By default it will be created using types (optionally) specified in columns.

        If overridden, use the provided connection object for setting up the table in order to
        create the table and insert data using the same transaction.
        """
        first_col = self.columns[0]
        if len(first_col) == 1 or isinstance(first_col, six.string_types):
            # only names of columns specified, no types
            raise NotImplementedError("create_table() not implemented for %r and columns types not specified" % self.table)
        elif len(first_col) == 2:
            # if columns is specified as (name, type) tuples
            coldefs = ','.join(
                '{name} {type}'.format(name=name, type=type) for name, type in self.columns
            )
            query = "CREATE TABLE {table} ({coldefs})".format(table=self.table,
                                                              coldefs=coldefs)
            connection.cursor().execute(query)

    @property
    def update_id(self):
        """
        This update id will be a unique identifier for this insert on this table.
        """
        return self.task_id

    def map_column(self, value):
        """
        Applied to each column of every row returned by `rows`.

        Default behaviour is to escape special characters and identify any self.null_values.
        """
        if value in self.null_values:
            return r'\\N'
        else:
            return default_escape(six.text_type(value))

    def run(self):
        """
        Inserts data generated by rows() into target table.

        If the target table doesn't exist, self.create_table will be called to attempt to create the table.

        Normally you don't want to override this.
        """
        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")

        connection = self.output().connect()
        # transform all data generated by rows() using map_column and write data
        # to a temporary file for import using postgres COPY
        tmp_dir = luigi.configuration.get_config().get('postgres', 'local-tmp-dir', None)
        tmp_file = tempfile.TemporaryFile(dir=tmp_dir)
        n = 0
        for row in self.rows():
            n += 1
            if n % 100000 == 0:
                logger.info("Wrote %d lines", n)
            rowstr = self.column_separator.join(self.map_column(val) for val in row)
            rowstr += "\n"
            tmp_file.write(rowstr.encode('utf-8'))

        logger.info("Done writing, importing at %s", datetime.datetime.now())
        tmp_file.seek(0)

        # attempt to copy the data into postgres
        # if it fails because the target table doesn't exist
        # try to create it by running self.create_table
        for attempt in range(2):
            try:
                cursor = connection.cursor()
                self.init_copy(connection)
                self.copy(cursor, tmp_file)
                self.post_copy(connection)
            except psycopg2.ProgrammingError as e:
                if e.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE and attempt == 0:
                    # if first attempt fails with "relation not found", try creating table
                    logger.info("Creating table %s", self.table)
                    connection.reset()
                    self.create_table(connection)
                else:
                    raise
            else:
                break

        # mark as complete in same transaction
        self.output().touch(connection)

        # commit and clean up
        connection.commit()
        connection.close()
        tmp_file.close()


class PostgresQuery(luigi.task.MixinNaiveBulkComplete, luigi.Task):
    """
    Template task for querying a Postgres compatible database

    Usage:
    Subclass and override the required `table`, `query` and `dsn` attributes.

    Override the `run` method if your use case requires some action with the query result.

    Task instances require a dynamic `update_id`, e.g. via parameter(s), otherwise the query will only execute once

    To customize the query signature as recorded in the database marker table, override the `update_id` property.
    """

    @abc.abstractproperty
    def dsn(self):
        """Example usage: return postgres://user@host:port/dbname'"""
        return None

    @abc.abstractproperty
    def table(self):
        return None

    @abc.abstractproperty
    def query(self):
        return None

    def run(self):
        connection = self.output().connect()
        cursor = connection.cursor()
        sql = self.query

        logger.info('Executing query from task: {name}'.format(name=self.__class__))
        cursor.execute(sql)

        # Update marker table
        self.output().touch(connection)

        # commit and close connection
        connection.commit()
        connection.close()

    def output(self):
        """
        Returns a PostgresTarget representing the executed query.

        Normally you don't override this.
        """
        return PostgresTarget(
            table=self.table,
            update_id=self.update_id,
            dsn=self.dsn
        )

    @property
    def update_id(self):
        """
        Override to create a custom marker table 'update_id' signature for Query subclass task instances
        """
        return self.task_id
