import datetime as dt

import psycopg
import psycopg.rows

import simplejson
from psycopg.types.json import JsonDumper, JsonbDumper, JsonLoader, Jsonb
from psycopg.types.datetime import DateLoader, DateDumper, TimestamptzLoader

# JSON dumper / loader for decimal support

class SimpleJsonDumper(JsonDumper):
    def dump(self, obj):
        return simplejson.dumps(obj, use_decimal=True).encode('utf-8')


class SimpleJsonbDumper(JsonbDumper):
    def dump(self, obj):
        return simplejson.dumps(obj, use_decimal=True).encode('utf-8')


class SimpleJsonLoader(JsonLoader):
    def load(self, data):
        # Handle bytes, memoryview, or any bytes-like object
        if not isinstance(data, str):
            data = bytes(data).decode('utf-8')
        return simplejson.loads(data, use_decimal=True)


# Date dumper / loader for infinity support

class InfDateDumper(DateDumper):
    def dump(self, obj):
        if obj == dt.date.max:
            return b"infinity"
        elif obj == dt.date.min:
            return b"-infinity"
        else:
            return super().dump(obj)

class InfDateLoader(DateLoader):
    def load(self, data):
        if data == b"infinity":
            return dt.date.max
        elif data == b"-infinity":
            return dt.date.min
        else:
            return super().load(data)

# Timestamptz loader for infinity support

class InfTimestamptzLoader(TimestamptzLoader):
    def load(self, data):
        if data == b"infinity":
            return "infinity"
        elif data == b"-infinity":
            return "-infinity"
        else:
            return super().load(data)


class DBConn:
    """Wrapper for database connection providing execute and query methods."""

    def __init__(self, conn):
        self._conn = conn

    async def execute(self, query, **kwargs):
        """Execute a query without returning results."""
        result = await self._conn.execute(query, kwargs)
        await result.close()

    async def execute_many(self, query, rows):
        """Execute a query multiple times with different parameters."""
        async with self._conn.cursor() as cur:
            await cur.executemany(query, rows)

    async def query(self, query, **kwargs):
        """Execute a query and return all results."""
        results = await self._conn.execute(query, kwargs)
        output = await results.fetchall()
        await results.close()
        return output


async def connect(*args, host, port=None, dbname, user=None, password=None, **kwargs):
    conn_string = ' '.join(
        f'{key}={value}' for key, value in [
            ('host', host),
            ('port', port),
            ('dbname', dbname),
            ('user', user),
            ('password', password)
        ] if value is not None
    )

    db_conn = await psycopg.AsyncConnection.connect(
        conn_string,
        autocommit=True,
        row_factory=psycopg.rows.dict_row
    )

    # Register dumpers for date, dict and Jsonb types
    db_conn.adapters.register_dumper(dt.date, InfDateDumper)
    db_conn.adapters.register_dumper(dict, SimpleJsonDumper)
    db_conn.adapters.register_dumper(Jsonb, SimpleJsonbDumper)

    # Register loaders
    db_conn.adapters.register_loader("json", SimpleJsonLoader)
    db_conn.adapters.register_loader("jsonb", SimpleJsonLoader)
    db_conn.adapters.register_loader("date", InfDateLoader)
    db_conn.adapters.register_loader("timestamptz", InfTimestamptzLoader)

    return DBConn(db_conn)
