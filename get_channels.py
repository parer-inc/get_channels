"""This service allows to get new tasks form database"""
import os
import sys
import time
import MySQLdb
from redis import Redis
from rq import Worker, Queue, Connection

def get_cursor():
    """Returns database cursor"""
    try:
        mydb = MySQLdb.connect(
            host="database",
            password=os.environ['MYSQL_ROOT_PASS'],
            database='youpar'
        )
    except MySQLdb.Error as error:
        print(error)
        sys.exit("Error: Failed connecting to database")
    return mydb.cursor()

def get_redis():
    """Returns redis connection"""
    try:
        redis = Redis(host='redis', port=6379)
    except Redis.DoesNotExist as error:
        print(error)
        sys.exit("Error: Faild connecting to redis")
    return redis


def get_channels(type=None, col=None, value=None):
    """Returns channels info from databse (table channels)"""
    cursor = get_cursor()
    q = '''SELECT * FROM channels '''
    if type is not None:
        value = value.replace(";", "")
        value = value.replace("'", "''")
        if type == "WHERE":
            value
            q += f'''WHERE {col} = "{value}"'''
    try:
        cursor.execute(q)
    except MySQLdb.Error as error:
        print(error)
        sys.exit("Error:Failed getting new tasks from database")
    data = cursor.fetchall()
    cursor.close()
    return data


if __name__ == '__main__':
    time.sleep(5)
    r = get_redis()
    q = Queue('get_channels', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r,  name='get_channels')
        worker.work()
