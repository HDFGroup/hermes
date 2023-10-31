
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
        return f'{id.unique}.{id.node_id}'

    def collect(self):
        mdm = self.hermes.CollectMetadataSnapshot()
        for blob in mdm.blob_info:
            blob_info = {
                'name': str(blob.get_name()),
                'id': self.unique(blob.blob_id),
                'score': float(blob.score),
                'access_frequency': 0,
                'buffer_info': []
            }
            for buf in blob.buffers:
                buf_info = {
                    'target_id': self.unique(buf.tid),
                    'size': int(buf.t_size)
                }
                blob_info['buffer_info'].append(buf_info)
            self.blob_info.append(blob_info)
        for tag in mdm.bkt_info:
            tag_info = {
                'id': self.unique(tag.tag_id),
                'name': str(tag.get_name()),
                'blobs': [self.unique(blob.blob_id) for blob in tag.blobs]
            }
            self.tag_info.append(tag_info)
        for target in mdm.target_info:
            target_info = {
                'name': None,
                'id':  self.unique(target.tgt_id),
                'rem_cap': target.rem_cap,
                'max_cap': target.max_cap,
                'bandwidth': target.bandwidth,
                'latency': target.latency,
                'score': target.score,
            }
            self.target_info.append(target_info)
        self.target_info.sort(reverse=True, key=lambda x: x['bandwidth'])
        for i, target in enumerate(self.target_info):
            target['name'] = f'Tier {i}'

# mdm = MetadataSnapshot()
# mdm.collect()
# print('Done')
