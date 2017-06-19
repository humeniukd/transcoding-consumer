from subprocess import Popen, PIPE
import logging, sys, psycopg2
from logging import handlers
from datetime import datetime, timedelta
from wsgiref.simple_server import make_server
from json import loads
from boto import sqs, s3
from re import U, I, compile as recompile
from threading import Thread
from os import remove, environ
from math import floor
from boto.s3.key import Key
from boto.sqs.message import RawMessage

MAX_THREADS = 4
HEIGHT = 140
WIDTH = 1800
AWS_REGION = environ['AWS_REGION']
AWS_ACCESS_KEY_ID = environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = environ['AWS_SECRET_ACCESS_KEY']
QUEUE_NAME = environ['QUEUE_NAME']
IN_BUCKET_NAME = environ['IN_BUCKET_NAME']
OUT_BUCKET_NAME = environ['OUT_BUCKET_NAME']

#rds settings
rds_host = environ['RDS_HOSTNAME']
name = environ['RDS_USERNAME']
password = environ['RDS_PASSWORD']
db_name = environ['RDS_DB_NAME']
db_port = environ['RDS_PORT']

WORK_DIR = environ['WORK_DIR']
LOG_LEVEL = getattr(logging, environ['LOG_LEVEL'])

# Create logger
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

# Handler
LOG_FILE = environ['LOG_FILE']
handler = handlers.RotatingFileHandler(LOG_FILE, maxBytes=1048576, backupCount=5)
handler.setLevel(LOG_LEVEL)

# Formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Add Formatter to Handler
handler.setFormatter(formatter)

# add Handler to Logger
logger.addHandler(handler)

welcome = """
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <!--
    Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.Amazon/apache2.0/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
  -->
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Welcome</title>
  <style>
  body {
    color: #ffffff;
    background-color: #E0E0E0;
    font-family: Arial, sans-serif;
    font-size:14px;
    -moz-transition-property: text-shadow;
    -moz-transition-duration: 4s;
    -webkit-transition-property: text-shadow;
    -webkit-transition-duration: 4s;
    text-shadow: none;
  }
  body.blurry {
    -moz-transition-property: text-shadow;
    -moz-transition-duration: 4s;
    -webkit-transition-property: text-shadow;
    -webkit-transition-duration: 4s;
    text-shadow: #fff 0px 0px 25px;
  }
  a {
    color: #0188cc;
  }
  .textColumn, .linksColumn {
    padding: 2em;
  }
  .textColumn {
    position: absolute;
    top: 0px;
    right: 50%;
    bottom: 0px;
    left: 0px;

    text-align: right;
    padding-top: 11em;
    background-color: #1BA86D;
    background-image: -moz-radial-gradient(left top, circle, #6AF9BD 0%, #00B386 60%);
    background-image: -webkit-gradient(radial, 0 0, 1, 0 0, 500, from(#6AF9BD), to(#00B386));
  }
  .textColumn p {
    width: 75%;
    float:right;
  }
  .linksColumn {
    position: absolute;
    top:0px;
    right: 0px;
    bottom: 0px;
    left: 50%;

    background-color: #E0E0E0;
  }

  h1 {
    font-size: 500%;
    font-weight: normal;
    margin-bottom: 0em;
  }
  h2 {
    font-size: 200%;
    font-weight: normal;
    margin-bottom: 0em;
  }
  ul {
    padding-left: 1em;
    margin: 0px;
  }
  li {
    margin: 1em 0em;
  }
  </style>
</head>
<body id="sample">
  <div class="textColumn">
    <h1>Congratulations</h1>
    <p>Your first AWS Elastic Beanstalk Python Application is now running on your own dedicated environment in the AWS Cloud</p>
  </div>
  
  <div class="linksColumn"> 
    <h2>What's Next?</h2>
    <ul>
    <li><a href="http://docs.amazonwebservices.com/elasticbeanstalk/latest/dg/">AWS Elastic Beanstalk overview</a></li>
    <li><a href="http://docs.amazonwebservices.com/elasticbeanstalk/latest/dg/index.html?concepts.html">AWS Elastic Beanstalk concepts</a></li>
    <li><a href="http://docs.amazonwebservices.com/elasticbeanstalk/latest/dg/create_deploy_Python_django.html">Deploy a Django Application to AWS Elastic Beanstalk</a></li>
    <li><a href="http://docs.amazonwebservices.com/elasticbeanstalk/latest/dg/create_deploy_Python_flask.html">Deploy a Flask Application to AWS Elastic Beanstalk</a></li>
    <li><a href="http://docs.amazonwebservices.com/elasticbeanstalk/latest/dg/create_deploy_Python_custom_container.html">Customizing and Configuring a Python Container</a></li>
    <li><a href="http://docs.amazonwebservices.com/elasticbeanstalk/latest/dg/using-features.loggingS3.title.html">Working with Logs</a></li>

    </ul>
  </div>
</body>
</html>
"""

re_duration = recompile('Duration: (\d{2}):(\d{2}):(\d{2}).(\d{2})[^\d]*', U)
re_freq = recompile('(\d+) Hz', U | I)
re_position = recompile('out_time_ms=(\d+)\d{3}', U)

delta = timedelta(seconds=2)

def time2ms(s):
    hours = 3600000 * int(s.group(1))
    minutes = 60000 * int(s.group(2))
    seconds = 1000 * int(s.group(3))
    ms = 10 * int(s.group(4))
    return hours + minutes + seconds + ms

def ratio(position, duration):
    if not position or not duration:
        return 0
    percent = int(floor(100 * position / duration))
    return 100 if percent > 100 else percent

COUNT = 0

class WfThread(Thread):
    global AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, IN_BUCKET_NAME, WORK_DIR, WIDTH, HEIGHT
    __SQSQueue = None
    __S3Conn = None
    __conn = None

    @property
    def SQSQueue(self):
        if None == self.__SQSQueue:
            self.__SQSQueue = sqs.connect_to_region(AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID,
                                                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY).create_queue(self.__key)
        return self.__SQSQueue

    @property
    def conn(self):
        if None == self.__conn:
            try:
                self.__conn = psycopg2.connect(host=rds_host, user=name, password=password, dbname=db_name, port=db_port)
            except Exception as e:
                logger.error("Could not connect to Sql instance: %s" % e)
                sys.exit()
        return self.__conn

    @property
    def S3Conn(self):
        if None == self.__S3Conn:
            self.__S3Conn = s3.connect_to_region(AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY_ID,
                                     aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        return self.__S3Conn

    def __init__(self, key=None):
        global COUNT
        self.__key = key
        self.__spl = 0
        self.__outFile = key + '.mp3'
        self.__mJsonFile = key + '_m.json'
        COUNT += 1
        super(WfThread, self).__init__(name=self.__key)

    def __del__(self):
        global COUNT
        COUNT -= 1
        for fileName in [self.__mJsonFile, self.__key, self.__outFile]:
            remove(WORK_DIR + fileName)
        logger.info('Destruct %s', self.__key)

    def _set_daemon(self):
        return True

    def run(self):
        for fn in [self.__download, self.__probe, self.__process, self.__upload]:
            if not fn():
                self.__enqueue('{"key": "error"}')
                break

    def __probe(self):
        process = Popen([
            './ffprobe',
            '-i', WORK_DIR + self.__key
        ], stdout=PIPE, stderr=PIPE, bufsize=1)

        while True:
            output = process.stderr.readline().decode('utf-8')
            rc = process.poll()

            if output == '' and rc is not None:
                break

            logger.debug('Output probe %s', output)
            freq_match = re_freq.search(output)
            if freq_match:
                self.__freq = int(freq_match.group(1))

            duration_match = re_duration.search(output)
            if duration_match:
                duration = self.__duration = time2ms(duration_match)
                self.__enqueue('{"type": "duration", "value": %(value)d}' % {'value': duration})

        logger.info('probe RC: %d', rc)
        return 0 == rc

    def __process(self):
        spl = self.__duration*self.__freq/1000/WIDTH
        logger.info('duration: %d, freq: %d, spl: %d', self.__duration, self.__freq, spl)
        process = Popen([
            './ffmpeg',
            '-i', WORK_DIR + self.__key,
            '-map', '0:0',
            '-progress', '/dev/stderr',
            '-af', 'dumpwave=s=%dx%d:c=%d:json=%s' % (WIDTH, HEIGHT, spl, WORK_DIR + self.__mJsonFile),
            WORK_DIR + self.__outFile
        ], stdout=PIPE, stderr=PIPE, bufsize=1)

        ms = None
        percent = 0
        ts = datetime.now()

        while True:
            output = process.stderr.readline().decode('utf-8')
            rc = process.poll()

            if output == '' and rc is not None:
                break

            logger.debug('Output %s', output)

            if self.__duration:
                position_match = re_position.search(output)
                if position_match:
                    ms = int(position_match.group(1))

            _percent = ratio(ms, self.__duration)

            if _percent != percent:
                now = datetime.now()

                if now - ts > delta:
                    ts = now
                    percent = _percent
                    self.__enqueue('{"type": "percent", "value": %(value)d}' % {'value': percent})
        isOk = 0 == rc
        if isOk:
            with self.conn.cursor() as cur:
                cur.execute('UPDATE mcloud_track SET state = 3, duration = %s WHERE uid = %s', (self.__duration, self.__key))
                self.conn.commit()
            self.__enqueue('{"type": "percent", "value": %(value)d}' % {'value': 100})

        else:
            self.__enqueue('{"type": "error"}')
        logger.info('RC: %d', rc)
        return isOk

    def __enqueue(self, msg):
        m = RawMessage()
        m.set_body(msg)
        return self.SQSQueue.write(m)

    def __download(self):
        bucket = self.S3Conn.get_bucket(IN_BUCKET_NAME)
        obj = bucket.get_key(self.__key)
        if None == obj:
            logger.info('No key to download %s', self.__key)
            return False
        try:
            obj.get_contents_to_filename(WORK_DIR + self.__key)
        except Exception as e:
            logger.info('Download failed %s', str(e))
            return False
        logger.info('Downloaded %s', self.__key)
        return True

    def __upload(self):
        global WORK_DIR, OUT_BUCKET_NAME
        for fileName in [self.__mJsonFile, self.__outFile]:
            bucket = self.S3Conn.get_bucket(OUT_BUCKET_NAME)
            k = Key(bucket, fileName)
            try:
                k.set_contents_from_filename(WORK_DIR + fileName)
                k.set_acl('public-read')
            except Exception as e:
                logger.info('Upload failed: %s', str(e))
            logger.info('Uploaded %s', fileName)
        return True

def application(environ, start_response):
    path    = environ['PATH_INFO']
    method  = environ['REQUEST_METHOD']
    status = '200 OK'
    if method == 'POST':
        try:
            if path == '/':
                request_body_size = int(environ['CONTENT_LENGTH'])
                request_body = environ['wsgi.input'].read(request_body_size).decode()
                logger.info("Received message: %s" % request_body)
                json = loads(request_body)
                key = json['Records'][0]['s3']['object']['key']
                logger.debug("Received key: %s, %d" % (key, COUNT))
                WfThread(key).start()
            elif path == '/scheduled':
                logger.debug("Received task %s scheduled at %s", environ['HTTP_X_AWS_SQSD_TASKNAME'], environ['HTTP_X_AWS_SQSD_SCHEDULED_AT'])
        except Exception as e:
            logger.warning('Error doing work: %s' % e)
        response = ''
    else:
        response = welcome
    headers = [('Content-type', 'text/html')]

    start_response(status, headers)
    return [response]

if __name__ == '__main__':
    httpd = make_server('', 8000, application)
    print("Serving on port 8000...")
    httpd.serve_forever()
