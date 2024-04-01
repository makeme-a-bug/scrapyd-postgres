import json
import psycopg2
from datetime import datetime

try:
    from collections.abc import MutableMapping
except ImportError:
    from collections import MutableMapping

class JsonPostgresDict(MutableMapping):
    """SQLite-backed dictionary"""

    def __init__(self, database, user, password, host, port, table="dict"):
        self.conn = psycopg2.connect(f"user={user} password={password} host={host} port={port} dbname={database}")
        self.table = table
        self.conn.autocommit = True  # Needed for psycopg2
        self._create_table()

    def _create_table(self):
        with self.conn.cursor() as cur:
            q = "create table if not exists %s (key text primary key, value text)" \
            % self.table
            cur.execute(q)

    def __getitem__(self, key):
        key = self.encode(key)
        q = f"select value from {self.table} where key=%s"
        with self.conn.cursor() as cur:
            cur.execute(q, (key,))
            value = cur.fetchone()
            if value:
                return self.decode(value[0])
        raise KeyError(key)

    def __setitem__(self, key, value):
        key, value = self.encode(key), self.encode(value)
        q = f"insert into {self.table} (key, value) values (%s,%s) ON CONFLICT (key) DO UPDATE SET key = excluded.key,value = excluded.value;"
        self.conn.cursor().execute(q, (key, value))
        self.conn.commit()

    def __delitem__(self, key):
        key = self.encode(key)
        q = f"delete from {self.table} where key=%s"
        self.conn.cursor().execute(q, (key,))
        self.conn.commit()

    def __len__(self):
        q = "select count(*) from %s" % self.table
        with self.conn.cursor() as cur:
            cur.execute(q)
            return cur.fetchone()[0]

    def __iter__(self):
        for k in self.iterkeys():
            yield k

    def iterkeys(self):
        q = "select key from %s" % self.table
        with self.conn.cursor() as cur:
            cur.execute(q)
        return (self.decode(x[0]) for x in cur.fetchall())

    def keys(self):
        return list(self.iterkeys())

    def itervalues(self):
        q = "select value from %s" % self.table
        with self.conn.cursor() as cur:
            cur.execute(q)
        return (self.decode(x[0]) for x in cur.fetchall())

    def values(self):
        return list(self.itervalues())

    def iteritems(self):
        q = "select key, value from %s" % self.table
        with self.conn.cursor() as cur:
            cur.execute(q)
            return ((self.decode(x[0]), self.decode(x[1])) for x in cur.fetchall())

    def items(self):
        return list(self.iteritems())

    def encode(self, obj):
        return json.dumps(obj)

    def decode(self, obj):
        return json.loads(obj)

class JsonPostgresPriorityQueue(object):
    """SQLite priority queue. It relies on SQLite concurrency support for
    providing atomic inter-process operations.
    """

    def __init__(self, database, user, password, host, port, table="queue"):
        print(database,user, password,host,port)
        self.conn = psycopg2.connect(f"user={user} password={password} host={host} port={port} dbname={database}")
        self.table = table
        print(self.conn)
        print("table created")
        self.conn.autocommit = True  # Needed for psycopg2
        self._create_table()

    def _create_table(self):
        with self.conn.cursor() as cur:
            q = "create table if not exists %s (id SERIAL primary key , " \
            "priority integer, message text)" % self.table
            cur.execute(q)
            print("table created")

    def put(self, message, priority=0.0):
        args = (priority, self.encode(message))
        q = f"insert into {self.table} (priority, message) values (%s,%s)"
        with self.conn.cursor() as cur:
            cur.execute(q, args)

    def pop(self):
        q = "select id, message from %s order by priority desc limit 1" \
            % self.table
        with self.conn.cursor() as cur:
            cur.execute(q)
            idmsg = cur.fetchone()
            if idmsg is None:
                return
            id, msg = idmsg
            q = f"delete from {self.table} where id=%s"
            c = cur.execute(q, (id,))
            if not cur.rowcount:  # record vanished, so let's try again
                self.conn.rollback()
                return self.pop()
            self.conn.commit()
        return self.decode(msg)

    def remove(self, func):
        q = "select id, message from %s" % self.table
        n = 0
        with self.conn.cursor() as cur:
            cur.execute(q)
            for id, msg in cur.fetchall():
                if func(self.decode(msg)):
                    q = f"delete from {self.table} where id=%s"
                    c = cur.execute(q, (id,))
                    if not cur.rowcount:  # record vanished, so let's try again
                        self.conn.rollback()
                        return self.remove(func)
                    n += 1
            self.conn.commit()
        return n

    def clear(self):
        with self.conn.cursor() as cur:
            cur.execute("delete from %s" % self.table)
            self.conn.commit()

    def __len__(self):
        q = "select count(*) from %s" % self.table
        with self.conn.cursor() as cur:
            cur.execute(q)
            return cur.fetchone()[0]

    def __iter__(self):
        q = "select message, priority from %s order by priority desc" % \
            self.table
        with self.conn.cursor() as cur:
            cur.execute(q)
            return ((self.decode(x), y) for x, y in cur.fetchall())

    def encode(self, obj):
        return json.dumps(obj)

    def decode(self, text):
        return json.loads(text)

class PostgresFinishedJobs(object):
    """SQLite finished jobs."""

    def __init__(self, database, user, password, host, port, table="finished_jobs"):
        self.conn = psycopg2.connect(f"user={user} password={password} host={host} port={port} dbname={database}")
        print("table created")
        self.table = table
        self.conn.autocommit = True  # Needed for psycopg2
        self._create_table()

    def _create_table(self):
        with self.conn.cursor() as cur:
            print("table created")
            cur.execute(f"CREATE TABLE IF NOT EXISTS {self.table} (id SERIAL PRIMARY KEY, project TEXT, spider TEXT, job TEXT, start_time TIMESTAMP, end_time TIMESTAMP)")

    def add(self, job):
        args = (job.project, job.spider, job.job, job.start_time, job.end_time)
        q = f"insert into {self.table} (project, spider, job, start_time, end_time) values (%s,%s,%s,%s,%s)"
        with self.conn.cursor() as cur:
            cur.execute(q, args)

    def clear(self, finished_to_keep=None):
        w = ""
        if finished_to_keep:
            limit = len(self) - finished_to_keep
            if limit <= 0:
                return  # nothing to delete
            w = "where id <= (select max(id) from " \
                "(select id from %s order by end_time limit %d))" % (self.table, limit)
        q = "delete from %s %s" % (self.table, w)
        with self.conn.cursor() as cur:
            cur.execute(q)

    def __len__(self):
        q = "select count(*) from %s" % self.table
        with self.conn.cursor() as cur:
            cur.execute(q)
            return cur.fetchone()[0]

    def __iter__(self):
        q = "select project, spider, job, start_time, end_time from %s order by end_time desc" % \
            self.table
        with self.conn.cursor() as cur:
            cur.execute(q)
            return ((j[0], j[1], j[2],j[3],j[4]) for j in cur.fetchall())