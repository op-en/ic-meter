# coding: utf-8
from Ic import *
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime,timedelta
import os, time
import logging
logging.basicConfig()

config = {
    'interval': int(os.environ.get('POLL_INTERVAL',15)),
    'database': os.environ.get('DATABASE_NAME','SURE'),
    'ic_user': os.environ.get('IC_USER','user'),
    'ic_password': os.environ.get('IC_PASSWORD','password'),
    'influx_host': os.environ.get('INFLUX_HOST','192.168.99.100'),
    'influx_port': int(os.environ.get('INFLUX_PORT',8086)),
    'influx_user': os.environ.get('INFLUX_USER','root'),
    'influx_password': os.environ.get('INFLUX_PASSWORD','root'),
    'verbose': (os.environ.get('VERBOSE','False') == 'True')
}

# Set the timezone, usees environment variable 'TZ', like this: os.environ['TZ']
time.tzset()
print('type=info msg="influx settings" host=%s database=%s' % (str(config['influx_host']), config['database'] ))

ic = Ic_controller()
ic.verbose = config['verbose']
ic.user = config['ic_user']
ic.password = config['ic_password']
ic.dbname = config['database']
ic.dbhost = config['influx_host']
ic.dbport = config['influx_port']
ic.dbuser = config['influx_user']
ic.dbpassword = config['influx_password']

def load_data():
    ic.login()
    ic.load_boxes()

# Run once on startup
print('type=info msg="running once on startup"')
load_data()
print('type=info msg="startup job finished"')

sched = BlockingScheduler()
@sched.scheduled_job('interval', seconds=config['interval'])
def poll_data():
    print('type=info msg="polling ic-meter" time="%s"' % datetime.now())
    load_data()

print('type=info msg="polling scheduled" interval=%s verbose=%s' % (str(config['interval']), config['verbose'] ))
# Start the schedule
sched.start()
