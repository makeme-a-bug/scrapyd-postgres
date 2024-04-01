from zope.interface import implementer

from scrapyd.interfaces import ISpiderQueue
from scrapyd.sqlite import JsonSqlitePriorityQueue
from scrapyd.postgres import JsonPostgresPriorityQueue
from scrapyd.utils import sqlite_connection_string


@implementer(ISpiderQueue)
class SqliteSpiderQueue(object):

    def __init__(self, config, project, table='spider_queue'):
        if config.get('db_type', 'scrapyd') == 'postgres':
            self.q=JsonPostgresPriorityQueue(database=config.get('db_database', 'scrapyd'),
            user=config.get('db_user', 'scrapyd'),
            password=config.get('db_password', 'scrapyd'),
            host=config.get('db_host', 'localhost'),
            port=config.getint('db_port', 5432),
            table=table)
        else:
            self.q = JsonSqlitePriorityQueue(sqlite_connection_string(config, project), table)

    def add(self, name, priority=0.0, **spider_args):
        d = spider_args.copy()
        d['name'] = name
        self.q.put(d, priority=priority)

    def pop(self):
        return self.q.pop()

    def count(self):
        return len(self.q)

    def list(self):
        return [x[0] for x in self.q]

    def remove(self, func):
        return self.q.remove(func)

    def clear(self):
        self.q.clear()
