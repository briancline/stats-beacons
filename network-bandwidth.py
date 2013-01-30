#!/usr/bin/env python
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


class StatsBeacon(object):
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


class NetworkBandwidthBeacon(StatsBeacon):
    name = 'network-bandwidth'

    def beacon_init(self):
        self.ifstats = {}
        self.ignore_interfaces = self.get_val('ignore_interfaces')

    def poll(self):
        with open('/proc/net/dev', 'r') as f:
            output = f.read().splitlines()
        if not output:
            return

        print self.get_timestamp()
        for line in output:
            bits = line.replace(':', ': ', 1).strip().split()
            if not len(bits) or bits[0][-1:] != ':':
                continue

            interface = bits[0][:-1]
            if interface in self.ignore_interfaces:
                continue

            stats = {'rx.bytes':   long(bits[1]),
                     'rx.packets': long(bits[2]),
                     'rx.errors':  long(bits[3]),
                     'tx.bytes':   long(bits[9]),
                     'tx.packets': long(bits[10]),
                     'tx.errors':  long(bits[11])}

            if interface in self.ifstats:
                stats_diff = {}

                for metric in stats:
                    stats_diff[metric] = (stats[metric] -
                                          self.ifstats[interface][metric])

                self.send(interface, stats_diff)

            self.ifstats[interface] = stats

    def send(self, interface, stats):
        for stat in stats.keys():
            #print "sending %s" % {'host': self.get_hostname(),
            #                      'service': stat,
            #                      'metric': stats[stat]}
            self.riemann.send({'host': self.get_hostname(),
                               'service': stat,
                               'metric': stats[stat]})


if __name__ == "__main__":
    beacon = NetworkBandwidthBeacon()
    beacon.run()
