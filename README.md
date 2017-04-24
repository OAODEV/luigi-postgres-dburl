# luigi_postgres_dburl
[![CircleCI](https://circleci.com/gh/OAODEV/luigi-postgres-dburl/tree/master.svg?style=svg)](https://circleci.com/gh/OAODEV/luigi-postgres-dburl/tree/master)


**Goal**: Add support for Postgres dsn connection strings to Luigi.

The code from this repo is a slightly modified version of what's in
`luigi.contrib.postgres`, so any modules defined there as of luigi 2.6.1
should be available in this repo.

**Reason for repo existence**: Rather than needing host, user, password, and
database to connect to a database, the connection is defined by specifying a
dsn string, which will be directly interpreted by libpq. Psycopg2 can figure
out some connection elements through the environment, therefore sometimes not
all parameters are necessary.

Documentation on the format is in [postgres docs](https://www.postgresql.org/docs/9.5/static/libpq-connect.html) at section **31.1.1.2**.
