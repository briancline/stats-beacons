import os
import time
import datetime
import bernhard
from contextlib import contextmanager
from ConfigParser import ConfigParser


@contextmanager
def sliding_sleep_time(beacon):
    start = time.time()
    yield
    beacon.sleep_time = (beacon.poll_interval -
                         (beacon.sleep_time / 1000) -
                         (time.time() - start))


class PollingBeacon(object):
    name = 'beacon'
    config_file = 'beacons.ini'

    def __init__(self):
        self.config = ConfigParser()
        self.config.read(self.config_file)

        self.poll_interval = self.get_val_int('poll_interval')
        self.sleep_time = self.poll_interval

        self.riemann = bernhard.Client(host=self.config.get('riemann', 'host'),
                                       port=self.config.getint('riemann',
                                                               'port'),
                                       transport=bernhard.UDPTransport)

        self.beacon_init()

    def get_val(self, key):
        return self.config.get(self.name, key)

    def get_val_int(self, key):
        return self.config.getint(self.name, key)

    def get_timestamp(self):
        now = datetime.datetime.now()
        return (now.strftime('%Y-%m-%d %H:%M:%S.%%(msec)03d %z') %
                {'msec': int(now.strftime('%f')) / 1000})

    def get_hostname(self):
        return os.uname()[1]

    def run(self):
        while True:
            with(sliding_sleep_time(self)):
                self.poll()

            time.sleep(self.sleep_time)

    def poll(self):
        raise Exception('Jane, stop this crazy thing! Define a poll() method.')
