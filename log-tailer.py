#!/usr/bin/env python
import time
import subprocess
from beacon import PollingBeacon


class LogTailerBeacon(PollingBeacon):
    name = 'log-tailer'

    def beacon_init(self):
        self.service = self.get_val('service_name')
        self.filename = self.get_val('file_name')
        self.proc = subprocess.Popen(['tail', '-F', self.filename],
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)

    def poll(self):
        while True:
            line = self.proc.stdout.readline()
            print line
            self.send(line)

    def send(self, line):
        self.riemann.send({'host': self.get_hostname(),
                           'service': self.service,
                           'state': "ok",
                           'time': time.time(),
                           'metric': 0,
                           'description': line})


if __name__ == '__main__':
    beacon = LogTailerBeacon()
    beacon.run()
