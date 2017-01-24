#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example Kafka Producer.
# Reads lines from stdin and sends to Kafka.
#

from confluent_kafka import Producer
import sys
import string
import random
import thread
import time
import json
from pprint import pprint

# Random string generator
def string_generator(size=10, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def delivery_callback (err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d]\n' % (msg.topic(), msg.partition()))


def produce(conf,data) :
    p = Producer(**conf)

    counter = 0
    while 1:
        line=data['source']+":hello-"+str(counter)
        counter+=1
        try:
            p.produce(data['topic'], line, callback=delivery_callback)
            time.sleep(.1)
        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full ' \
                             '(%d messages awaiting delivery): try again\n' %
                             len(p))
        p.poll(0)
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()

        
if __name__ == '__main__':
    if not  (2 <= len(sys.argv) <= 3):
        sys.stderr.write('Usage: %s <bootstrap-brokers> [ <topic1>,<topic2>,... ]\n' % sys.argv[0])
        sys.stderr.write('\topics must be separated by a \',\' and there must be no whitespaces\n')
        sys.exit(1)

    broker = sys.argv[1]
        
    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': broker, 'message.max.bytes':1000000000}

    with open('msg-conf-new-01.json') as data_file:    
        data = json.load(data_file)

    try:
        for i in data["streams"]:
            thread.start_new_thread( produce, (conf,i) )
    except:
        print "Error: unable to start thread"

    while 1:
        pass
