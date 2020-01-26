# pgToSqlite
A C++ tool to dump a PostgreSQL database to SQLite3

[![Build Status](https://travis-ci.org/BGO-OD/pgToSqlite.svg?branch=master)](https://travis-ci.org/BGO-OD/pgToSqlite)

This tool allows to dump a full / parts of a [PostgreSQL](https://www.postgresql.org/) database into an [SQLite3](https://www.sqlite.org/) database.

It can handle [PostgreSQL large objects](https://www.postgresql.org/docs/12/largeobjects.html) (converted to blobs) and applies special semantics to special data types (such as dates, e.g. converting `infinity::timestamp` into `9999-12-31 12:00:00`) for maximum compatibility.

Furthermore, `autoincrement` columns are converted into an `UPDATE` trigger, indices are recreated and the final database is `ANALYZE`d for maximum performance.

It makes use of the [OptionParser](https://github.com/BGO-OD/OptionParser) to simplify argument parsing and config file handling.
