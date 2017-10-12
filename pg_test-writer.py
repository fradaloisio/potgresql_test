import os
import json
import logging
import logging.config
import random
import string
import pg8000
import threading


def setup_logging(
    default_path='logging.json',
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


setup_logging()
logger = logging.getLogger(__name__)

def get_connection():
    conn = pg8000.connect(user="postgres", password="password",
                          host="172.17.0.3", port=5432, database="test")
    return conn
def stampa():
    N=5
    while True:
        logger.info(str(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))))

def write():
    N = 500
    conn = get_connection()
    cur = conn.cursor()
    while True:
        content = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))
        q = "INSERT INTO data (content) VALUES ('"+content+"') RETURNING id, COALESCE(to_char(creationdate, 'MM-DD-YYYY HH24:MI:SS.MS'), '') AS creationdate, content;"
        cur.execute(q)
        for row in cur:
            id, creationdate, content = row
            logger.info("ID: %s, CREATIONDATE: %s, CONTENT: %s" % (id, creationdate,content))
        conn.commit()

def read():
    conn = get_connection()
    cur = conn.cursor()
    (cur.execute("SELECT id, COALESCE(to_char(creationdate, 'MM-DD-YYYY HH24:MI:SS.MS'), '') AS creationdate, content from data order by creationdate desc limit 1;"))
    results = cur.fetchall()
    for r in results:
        logger.info("ID: %d, CREATIONDATE: %s, CONTENT: %s" %(int(r[0]),str(r[1]),r[2]))

def main():
    logger.info('Start writer')
    threads = []
    for i in range(10):
        t = threading.Thread(target=write, args=())
        threads.append(t)
        t.start()


if __name__ == '__main__':
    main()
