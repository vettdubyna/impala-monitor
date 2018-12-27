import re


class ImpalaStats(object):
    ITEMS_TO_TRACK = [
        'admission-controller.*',
        'jvm.total.*',
        'impala.thrift-server.*',
        'impala-server.num-queries',
        'impala-server.num-queries-expired',
        'memory.rss',
        'memory.total-used',
        'tcmalloc.*'
    ]

    def __init__(self, statsd):
        self._statsd = statsd

    def send(self, node, payload):
        #print(payload)
        for pattern in self.ITEMS_TO_TRACK:
            for key in payload['metric_group']['metrics']:
                #print("keyname = " + key["name"] + "\n")
                if re.match(pattern, str(key['name'])):
                    print("Regex matches " + key['name'] + "\tvalue: " + str(key['value']) + " node: " + node)
                    extended_key = "{}.{}".format(node.replace(':25000', '').replace('.', '_'), key["name"])
                    self._statsd.gauge(extended_key, int(key["value"]))

            for item in payload['metric_group']['child_groups']:
                for key in item['metrics']:
                    #print("keyname = " + key["name"] + "\n")
                    if re.match(pattern, str(key['name'])):
                        print("Regex matches #2 " + key["name"] + "\tvalue: " + str(key['value']) + " node: " + node)
                        extended_key = "{}.{}".format(node.replace(':25000', '').replace('.', '_'), key["name"])
                        self._statsd.gauge(extended_key, int(key["value"]))
