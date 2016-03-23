# coding: utf-8

import pandas as pd
import numpy as np
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
from influxdb.client import InfluxDBClientError
import math
import time
from datetime import datetime
import requests
import sys
import json
import os
import pytz
import re

class Timeseries:
    def __init__(self,series_name):

        self.name = series_name


    def SetInfluxConnection(self,dbhost='localhost',dbport=8086,dbuser='root', dbpassword='root',dbname='test'):

        if dbhost != None:
            self.influxdb = DataFrameClient(dbhost, dbport, dbuser, dbpassword, dbname)
            # self.influxdb2 = InfluxDBClient(dbhost, dbport, dbuser, dbpassword, dbname)
            self.dbname = dbname

    def CheckDatabase(self):
        try:
            self.influxdb.create_database(self.dbname)
        except InfluxDBClientError,e:
            if str(e) == "database already exists":
                return True

        return False

    def CheckSeries(self,prop = '*'):

        q = 'select %s from \"%s\" limit 1;' % (prop,self.name)
        result = self.influxdb.query(q)

        print q
        print result

        return result[self.name].index[0]

    def GetLastTimeStamp(self,prop = '*'):
        q = 'select %s from \"%s\" order by time desc limit 1;' % (prop,self.name)
        result = self.influxdb.query(q)
        return result[self.name].index[0]


    def WriteDB(self,df):
        return self.influxdb.write_points(df, self.name)


    def LoadAll(self,start=1199145600, stop = time.time(),period = 60 * 60 * 24 * 7):



        for f in range(start,stop,period):
            data = self.GetDataPeriod(data_id,start, period)
            self.WriteDB(data)

        return



class Ic(Timeseries):

    session = None
    verbose = True
    box = None
    tz = None

    def convert_to_df(self,json):

        numberOfRows = len(json['rows'])

        columns = []
        col_types = []
        for col in json['cols']:
            columns.append(col['id'])
            col_types.append(col['type'])

        # print columns
        # print col_types

        # create dataframe
        df = pd.DataFrame(index=np.arange(0, numberOfRows), columns=columns )

        # now fill it up row by row
        for x in np.arange(0, numberOfRows):
            #loc or iloc both work here since the index is natural numbers
            values = []
            # print json['rows'][x]['c']
            for i,value in enumerate(json['rows'][x]['c']):
                if col_types[i] == 'datetime':
                    # Javascript dates are indexed by 0 in the month, so we have to do this manually
                    m = re.findall(r'Date\((\d+)\,(\d+)\,(\d+)\,(\d+)\,(\d+)\,(\d+)\)', value['v'])[0]
                    row_time = self.tz.localize(datetime(int(m[0]),int(m[1]) + 1, int(m[2]),int(m[3]),int(m[4]),int(m[5]))).isoformat()
                    values.append(row_time)
                else:
                    if value['v'] == None:# 0 Gets parsed to none by the json engine?
                        values.append(float(0))
                    else:
                        values.append(float(value['v']))
            df.loc[x] = values


        #Convert date and set as index
        df.index = pd.to_datetime(df["realtime"])
        df = df.drop("realtime",1)
        df.index.name = "time"

        return df

    def GetDataPeriod(self, data_id, start = 1199145600, period = 60 * 60 * 24 * 7.0):

        tz = pytz.timezone(self.box['timezone'])
        utc = pytz.timezone('UTC')
        fromdate = tz.normalize(tz.localize(datetime.fromtimestamp(start))).astimezone(utc).isoformat().replace('+00:00','Z')
        todate = tz.normalize(tz.localize(datetime.fromtimestamp(start + period))).astimezone(utc).isoformat().replace('+00:00','Z')

        # print fromdate
        # print todate

        timestamp = int(round(time.time() * 1000))

        data_url = "https://app.ic-meter.com/icm/api/measurements/days/%s?access_token=%s&fromDate=%s&toDate=%s&forecast=false&_=%i"% (self.box['boxId'],self.session['access_token'],fromdate,todate,timestamp)

        # print data_url

        r = requests.get(data_url)

        #Retry if failed
        if r.status_code != 200:
            r = requests.get(data_url)
        if r.status_code != 200:
            r = requests.get(data_url)
        if r.status_code != 200:
            print "Request failed!"

        # print r.text
        json = r.json()

        # print json['cols']

        # print json

        # exit(1)

        return self.convert_to_df(json)

    def ImportMissing(self, period = 60 * 60 * 24 * 7):
        try:
            last = self.GetLastTimeStamp().value / 10 ** 9
        except:
            last = self.box['fromdate'] / 1000

        if 'lastMeasurementDate' in self.box:
            stop = self.box['lastMeasurementDate'] / 1000

        self.tz = pytz.timezone(self.box['timezone'])
        self.ImportAll(self.box['boxId'],last,stop,period)

        return

    def ImportAll(self,data_id = None, start = 1199145600, stop = None, period = 60 * 60 * 24 * 7):

        if data_id == None:
            data_id = self.name

        if stop == None:
            stop = int(time.time())

        now = time.time()
        count = 0

        if self.verbose:
            print "Downloading data from %i to %i" % (start,stop)

        for f in range(start,stop,period):

            completed = 100* (f - start)/(stop-start)
            delta = time.time() - now

            if self.verbose:
                if delta > 10:
                    timeleft = int(  (100 - completed) * (delta / completed)   )
                    print("\rCompleted: %0.0f%% (%i seconds left)" % (completed,timeleft)),
                else:
                    print("\rCompleted: %0.0f%% " % completed),

                sys.stdout.flush()


            data = self.GetDataPeriod(data_id,f,period)

            #Retry
            if type(data) != pd.core.frame.DataFrame :
                time.sleep(3)
                print "Retrying..."
                data = self.GetDataPeriod(data_id,f,period)

            if type(data) == pd.core.frame.DataFrame:

                # data = data[data.Status == "Verklig"]

                count += data.shape[0]

                # print count

                self.WriteDB(data)


        if stop - start == 0:
            print("type=info msg=\"No new data available, skipping.\"")
            return

        completed = (100* (f-start))/(stop-start)

        if self.verbose:
            print("\rCompleted: %0.0f%%  \r  " % completed),

            delta = time.time() - now

        print("\rtype=info msg=\"Task completed\" elapsed_time=%0.0f rows_written=%i" % (delta,count))


        return


class Ic_controller():

    session = None
    user = None
    password = None
    verbose = True
    heat_boxes = []

    #Login
    def login(self):
        #Set header
        headers={"user-agent": "curl/7.43.0"}

        url = "https://app.ic-meter.com/icm/oauth/token?client_id=trusted-client&grant_type=password&scope=read&username=%s&password=%s" % (self.user, self.password)

        #Request password cookie
        r = requests.get(url,headers=headers,allow_redirects=False)

        if r.status_code == 200:
            self.session = json.loads(r.text)
            self.heat_boxes = []
            if self.verbose:
                print self.session
        else:
            raise Exception(r.text)

    def load_boxes(self):

        millis = int(round(time.time() * 1000))
        url = "https://app.ic-meter.com/icm/api/boxlocations?access_token=%s&_=%s" % (self.session['access_token'], millis)

        #Request password cookie
        r = requests.get(url,allow_redirects=False)

        boxes = json.loads(r.text)

        for box in boxes:
            if box['heatCapacity'] != None:
                self.heat_boxes.append(box)

        self.import_all_boxes()

    def import_all_boxes(self):
        for box in self.heat_boxes:
            ic = Ic("ic-meter." + box['boxId'])
            ic.box = box
            ic.session = self.session
            ic.verbose = self.verbose
            ic.SetInfluxConnection(dbname=self.dbname,dbhost=self.dbhost, dbport=self.dbport, dbuser=self.dbuser,dbpassword=self.dbpassword)
            ic.CheckDatabase()
            ic.ImportMissing()
