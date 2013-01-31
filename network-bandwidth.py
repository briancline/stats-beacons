#!/usr/bin/env python
from beacon import PollingBeacon


class NetworkBandwidthBeacon(PollingBeacon):
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
            #print 'sending %s' % {'host': self.get_hostname(),
            #                      'service': stat,
            #                      'metric': stats[stat]}
            self.riemann.send({'host': self.get_hostname(),
                               'service': stat,
                               'metric': stats[stat]})


if __name__ == '__main__':
    beacon = NetworkBandwidthBeacon()
    beacon.run()
