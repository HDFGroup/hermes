import h5py
import numpy as np

path = '/tmp/hello.h5'
print('Beginning')
f = h5py.File(path, "w")
print('File created')
prefix = 'train'
for p in prefix:
    group = f.create_group(p)
    story_dict = {0:1, 1:2, 2:3}
    length = len(story_dict)
    images = list()
    for i in range(5):
        images.append(
            group.create_dataset('image{}'.format(i), (length,), dtype=h5py.vlen_dtype(np.dtype('uint8'))))
    sis = group.create_dataset('sis', (length,), dtype=h5py.string_dtype(encoding='utf-8'))
    dii = group.create_dataset('dii', (length,), dtype=h5py.string_dtype(encoding='utf-8'))
f.close()
