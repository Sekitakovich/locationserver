import psycopg2
from psycopg2.extras import DictCursor
from typing import Dict, List
from threading import Thread
from queue import Queue
import time
import logging
from datetime import datetime as dt


class DBsession(Thread):

    def __init__(self, *, host: str, port: int, database: str, username: str, password: str):

        super().__init__()
        self.daemon = True
        self.logger = logging.getLogger('Log')

        self.isReady: bool = False

        self.dateFormat = '%Y-%m-%d'
        self.timeFormat = '%H:%M:%S'
        self.datetimeFormat = '%s %s' % (self.dateFormat, self.timeFormat)

        url: str = 'postgresql://%s:%s@%s:%d/%s' % (username, password, host, port, database)
        try:
            self.db = psycopg2.connect(url)
        except psycopg2.Error as e:
            self.logger.error(msg=e)
        else:
            self.quePoint = Queue()
            self.cursor = self.db.cursor(cursor_factory=DictCursor)
            self.account: Dict[str, int] = {}
            self.buildAccountList()
            self.isReady = True

    def __del__(self):
        if self.isReady:
            self.cursor.close()
            self.db.close()

    def update(self, *, table: str, id: int, kv: dict) -> int:
        self.cursor.execute("begin")

        if id == 0:
            self.cursor.execute("lock table %s in exclusive mode" % table)
            query = "select count(*),max(id) as id from %s where id>0 and vf=false" % table
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if int(result['count']) == 0:
                query = "select max(id)+1 as id from %s" % table
                self.cursor.execute(query)
                result = self.cursor.fetchone()
                id = int(result['id'])
                query = "insert into %s(id) values(%d)" % (table, id)
                self.cursor.execute(query)
            else:
                id = int(result['id'])
                query = "update %s set vf=true where id=%d" % (table, id)
                self.cursor.execute(query)

        saveat: str = dt.now().strftime(self.datetimeFormat)
        kv['saveat'] = saveat
        setter: List[str] = []
        for k, v in kv.items():
            setter.append("%s='%s'" % (k, v))
        query = "update %s set %s where id=%d" % (table, ','.join(setter), id)
        self.cursor.execute(query)
        self.logger.debug(msg=query)

        self.cursor.execute("commit")
        return id

    def select(self, *, query: str) -> list:
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        return result

    def buildAccountList(self):

        query = 'select id,username from account where vf=true order by id asc'
        result = self.select(query=query)

        for row in result:
            self.account[row['username']] = row['id']

    def saveLocation(self, *, record: dict) -> bool:

        success: bool = False

        try:
            username: str = record['username']
            location: dict = record['location']
            must: dict = location['must']
            plus: dict = location['plus']
            account: int = 0
            if username in self.account.keys():
                account = self.account[username]
                # print('%s = %d' % (username, account))
                # print(location)
                jst = dt.strptime(must['jst'], self.datetimeFormat)
                kv = {
                    'lat': must['lat'],
                    'lng': must['lng'],
                    'ns': must['ns'],
                    'ew': must['ew'],
                    'sog': must['sog'],
                    'cog': must['cog'],
                    'alt': must['alt'],
                    'calcmode': must['mode'],
                    # 'utc': must['utc'],
                    'jst': jst.strftime(self.datetimeFormat),
                    'ymd': jst.strftime(self.dateFormat),
                    'hms': jst.strftime(self.timeFormat),
                    'kmh': plus['kmh'],
                    'pdop': plus['pdop'],
                    'vdop': plus['vdop'],
                    'hdop': plus['hdop'],
                    'sats': plus['sats'],
                    'username': username,
                    'account': account,
                }
                if self.update(table='location', id=0, kv=kv):
                    success = True
        except (IndexError, ValueError) as e:
            self.logger.error(msg=e)
        else:
            pass

        return success

    def run(self) -> None:
        while True:
            postbody = self.quePoint.get()
            for index, record in enumerate(postbody):
                # pprint(record)
                self.saveLocation(record=record)


if __name__ == '__main__':

    database: str = 'locationserver'

    host: str = 'localhost'
    port: int = 6666
    username: str = 'postgres'
    password: str = 'postgres'

    session = DBsession(host=host, port=port, database=database, username=username, password=password)

    if session.isReady:
        session.start()
        print(session.account)

        kv = {
            'ns': 'S',
            'ew': 'W',
            'username': '地雷太郎',
        }
        session.update(table='location', id=0, kv=kv)

        kv = {
            'ns': 'N',
            'ew': 'E',
            'username': '法策',
        }
        session.update(table='location', id=1, kv=kv)