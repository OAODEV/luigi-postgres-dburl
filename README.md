# luigi_postgres_dburl


Goal: Add support for connection strings for Postgres connections to Luigi.

The code from this repo is a slightly modified version of what's in
`luigi.contrib.postgres`, therefore anything defined there as of luigi 2.6.1
should be available in this repo.

Big change: Rather than needing host, user, password, and database to connect to a database, you only have to specify a dsn string, which will be directly interpreted by libpq.
Documentation on this is in [postgres docs](https://www.postgresql.org/docs/9.5/static/libpq-connect.html) at section 31.1.1.2.
