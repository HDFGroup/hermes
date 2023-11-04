
from py_hermes import Hermes, TRANSPARENT_HERMES
class MetadataSnapshot:
    def __init__(self):
        TRANSPARENT_HERMES()
        self.hermes = Hermes()
        self.blob_info = []
        self.target_info = []
        self.tag_info = []

    @staticmethod
    def unique(id):
        return f'{id.node_id}.{id.unique}'

    def collect(self):
        mdm = self.hermes.CollectMetadataSnapshot()
        tag_to_blob = {}
        tid_to_tgt = {}
        for target in mdm.target_info:
            target_info = {
                'name': None,
                'id':  self.unique(target.tgt_id),
                'node_id': int(target.node_id),
                'rem_cap': target.rem_cap,
                'max_cap': target.max_cap,
                'bandwidth': target.bandwidth,
                'latency': target.latency,
                'score': target.score,
            }
            self.target_info.append(target_info)
            tid_to_tgt[target_info['id']] = target_info
        self.target_info.sort(reverse=True, key=lambda x: x['bandwidth'])
        for i, target in enumerate(self.target_info):
            target['name'] = f'Tier {i}'
        for blob in mdm.blob_info:
            blob_info = {
                'name': str(blob.get_name()),
                'id': self.unique(blob.blob_id),
                'mdm_node': int(blob.blob_id.node_id),
                'tag_id': self.unique(blob.tag_id),
                'score': float(blob.score),
                'access_frequency': 0,
                'buffer_info': []
            }
            for buf in blob.buffers:
                buf_info = {
                    'target_id': self.unique(buf.tid),
                    'node_id': 0,
                    'size': int(buf.t_size)
                }
                buf_info['node_id'] = tid_to_tgt[buf_info['target_id']]['node_id']
                blob_info['buffer_info'].append(buf_info)
            self.blob_info.append(blob_info)
            if blob_info['tag_id'] not in tag_to_blob:
                tag_to_blob[blob_info['tag_id']] = []
            tag_to_blob[blob_info['tag_id']].append(blob_info['id'])
        for tag in mdm.bkt_info:
            tag_info = {
                'id': self.unique(tag.tag_id),
                'mdm_node': int(tag.tag_id.node_id),
                'name': str(tag.get_name()),
                # 'blobs': [self.unique(blob.blob_id) for blob in tag.blobs]
                'blobs': []
            }
            if tag_info['id'] in tag_to_blob:
                tag_info['blobs'] = tag_to_blob[tag_info['id']]
            self.tag_info.append(tag_info)

# mdm = MetadataSnapshot()
# mdm.collect()
# print('Done')
