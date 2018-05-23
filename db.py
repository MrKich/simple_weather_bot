import sqlite3

from contextlib import contextmanager


class DB:
    def __init__(self, filename):
        self.filename = filename

    @contextmanager
    def cursor(self):
        conn = sqlite3.connect(self.filename)
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        finally:
            cur.close()
            conn.close()
