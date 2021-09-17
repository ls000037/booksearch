from elasticsearch import Elasticsearch
es_con = Elasticsearch(['http:/192.172.9.198:8100', "http://192.172.9.199:8100", "http://192.172.9.200:8100"],
                       sniff_on_start=True, sniff_on_connection_fail=True, sniffer_timeout=60)