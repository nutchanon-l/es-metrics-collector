import json
import sys
import threading
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, ConnectionTimeout

class ESControl:
    def __init__(self, conf_file: str):
        # extract configuration file
        with open(conf_file, "r") as f:
            conf = json.loads(f.read())
        f.close()
        
        # define variables
        self.alias = conf["source"]['alias']
        self.read_timeout = int(conf["source"]['readTimeout'])
        self.timediff = conf["target"]['timezoneDiff']
            
        # define collector
        self.clusterHealthIndex = conf["collector"]["clusterHealth"]["indexPrefix"] if conf["collector"]["clusterHealth"]["enabled"] is True else None
        self.nodesStats = conf["collector"]["nodesStats"]["indexPrefix"] if conf["collector"]["nodesStats"]["enabled"] is True else None
        self.indicesStats = conf["collector"]["indicesStats"]["indexPrefix"] if conf["collector"]["indicesStats"]["enabled"] is True else None
        self.indicesStatus = conf["collector"]["indicesStatus"]["indexPrefix"] if conf["collector"]["indicesStatus"]["enabled"] is True else None
        self.shardAllocation = conf["collector"]["shardAllocation"]["indexPrefix"] if conf["collector"]["shardAllocation"]["enabled"] is True else None
            
        # connect
        self.es_src = self._connect(conf, "source")
        self.es_tar = self._connect(conf, "target")
            
    def _connect(self, conf: dict, conn_type: str) -> Elasticsearch:
        # connect to Elasticsearch cluster
        conn_url = conf[conn_type]['url']
        conn_port = conf[conn_type]['port']
        conn_user = conf[conn_type]['username']
        conn_pass = conf[conn_type]['password']
        
        es = Elasticsearch(
            conn_url,
            http_auth=(conn_user, conn_pass),
            scheme="https",
            port=conn_port,
        )
        if es.ping() is True:
            return es
        else:
            print("Could not connect Elasticsearch cluster: {}".format(conn_url))
            sys.exit(2)
        

def sink_to_target(es: Elasticsearch, timediff: bool, alias: str, index_prefix: str, data: dict):
    if timediff is True:
        current_time = (datetime.now() - timedelta(hours=7)).replace(second=0, microsecond=0)
    else:
        current_time = datetime.now().replace(second=0, microsecond=0)
    current_time_suffix = current_time.strftime("%Y.%m.%d")
    
    index_name = "{}-{}".format(index_prefix, current_time_suffix)
    data['@timestamp'] = current_time
    data['alias'] = alias
    es.index(index=index_name, doc_type='_doc', body=data)
    
def es_cluster_health(es_src: Elasticsearch, es_tar: Elasticsearch, read_timeout: int, timediff: bool, alias: str, index_prefix: str):
    try:
        res = es_src.cluster.health(request_timeout=read_timeout)
        sink_to_target(es_tar, timediff, alias, index_prefix, res)
    except ConnectionTimeout as c:
        print('Timeout: Could not query /_cluster/health in {} due to {}'.format(read_timeout, str(c)))
    except Exception as e:
        print('Exception: Could not query /_cluster/health due to {}'.format(str(e)))
        
def es_nodes_stats(es_src: Elasticsearch, es_tar: Elasticsearch, read_timeout: int, timediff: bool, alias: str, index_prefix: str):
    try:
        res = es_src.nodes.stats(request_timeout=read_timeout)
        for node_key in res['nodes']:
            node = res['nodes'][node_key]
            node['node_name'] = node_key
            node['cluster_name'] = res['cluster_name']
            sink_to_target(es_tar, timediff, alias, index_prefix, node)
    except ConnectionTimeout as c:
        print('Timeout: Could not query /_nodes/stats in {} due to {}'.format(read_timeout, str(c)))
    except Exception as e:
        print('Exception: Could not query /_nodes/stats due to {}'.format(str(e)))

def es_indices_stats(es_src: Elasticsearch, es_tar: Elasticsearch, read_timeout: int, timediff: bool, alias: str, index_prefix: str):
    try:
        res = es_src.indices.stats(request_timeout=read_timeout)
        for index_key in res['indices']:
            # skip metadata indices that start with '.'
            if index_key[0] == '.':
                continue
            
            index = res['indices'][index_key]
            index['index_name'] = index_key
            sink_to_target(es_tar, timediff, alias, index_prefix, index)
    except ConnectionTimeout as c:
        print('Timeout: Could not query /_all/_stats in {} due to {}'.format(read_timeout, str(c)))
    except Exception as e:
        print('Exception: Could not query /_all/_stats due to {}'.format(str(e)))

def es_indices_status(es_src: Elasticsearch, es_tar: Elasticsearch, read_timeout: int, timediff: bool, alias: str, index_prefix: str):
    try:
        res = es_src.cat.indices(h="health,status,index,shardsPrimary,shardsReplica,docsCount,docsDeleted,storeSize",format='json', request_timeout=read_timeout)
        for index in res:
            sink_to_target(es_tar, timediff, alias, index_prefix, index)
    except ConnectionTimeout as c:
        print('Timeout: Could not query /_cat/indices in {} due to {}'.format(read_timeout, str(c)))
    except Exception as e:
        print('Exception: Could not query /_cat/indices due to {}'.format(str(e)))

def es_shard_allocation(es_src: Elasticsearch, es_tar: Elasticsearch, read_timeout: int, timediff: bool, alias: str, index_prefix: str):
    try:
        res = es_src.cat.allocation(h="node,shards,diskIndices,diskUsed,diskAvail,diskTotal,diskPercent", bytes='b',format='json', request_timeout=read_timeout)
        for node in res:
            sink_to_target(es_tar, timediff, alias, index_prefix, node)
    except ConnectionTimeout as c:
        print('Timeout: Could not query /_cat/allocation in {} due to {}'.format(read_timeout, str(c)))
    except Exception as e:
        print('Exception: Could not query /_cat/allocation due to {}'.format(str(e)))

def main():
    es_obj = ESControl("config.json")
    
    if es_obj.clusterHealthIndex is not None:
        threading.Thread(target=es_cluster_health, args=[es_obj.es_src, es_obj.es_tar, es_obj.read_timeout, es_obj.timediff, es_obj.alias, es_obj.clusterHealthIndex]).start()

    if es_obj.nodesStats is not None:
        threading.Thread(target=es_nodes_stats, args=[es_obj.es_src, es_obj.es_tar, es_obj.read_timeout, es_obj.timediff, es_obj.alias, es_obj.nodesStats]).start()
    
    if es_obj.indicesStats is not None:
        threading.Thread(target=es_indices_stats, args=[es_obj.es_src, es_obj.es_tar, es_obj.read_timeout, es_obj.timediff, es_obj.alias, es_obj.indicesStats]).start()
    
    if es_obj.indicesStatus is not None:
        threading.Thread(target=es_indices_status, args=[es_obj.es_src, es_obj.es_tar, es_obj.read_timeout, es_obj.timediff, es_obj.alias, es_obj.indicesStatus]).start()
    
    if es_obj.shardAllocation is not None:
        threading.Thread(target=es_shard_allocation, args=[es_obj.es_src, es_obj.es_tar, es_obj.read_timeout, es_obj.timediff, es_obj.alias, es_obj.shardAllocation]).start()

if __name__ == "__main__":
    main()
