import numpy as np
import h5py

def write_hdf5(fname, dset, dset_name, mode):
    with h5py.File(fname, mode, swmr=False) as h5_file: #swmr=False has async issue
        h5_file.create_dataset(
                    dset_name,
                    data=dset)

    print(f"{dset_name} {dset.shape} write complete ...")

def read_hdf5(fname):
    mode='r'
    with h5py.File(fname, mode, swmr=False) as h5_file:
        dsets = h5_file.keys()
        for ds in dsets:
            tmp_d = h5_file.get(ds)
            print(f"{ds} {tmp_d.shape} read complete ...")


if __name__ == "__main__":
    d1 = np.random.random(size = (1000,20))
    d2 = np.random.random(size = (200,200))
    # print(d1.shape, d2.shape)

    fname = '/tmp/test_hermes/test_data.h5'

    write_hdf5(fname,d1,'dataset_1','w')
    # (fname,d2,'dataset_2','a')

    read_hdf5(fname)
