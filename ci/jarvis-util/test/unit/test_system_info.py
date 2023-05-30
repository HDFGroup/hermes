from jarvis_util.util.argparse import ArgParse
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.local_exec import LocalExecInfo
from jarvis_util.util.hostfile import Hostfile
from jarvis_util.introspect.system_info import Lsblk, \
    ListFses, FiInfo, Blkid, ResourceGraph, StorageDeviceType
from jarvis_util.util.size_conv import SizeConv
import pathlib
import itertools
from unittest import TestCase


class TestSystemInfo(TestCase):
    def test_lsblk(self):
        Lsblk(LocalExecInfo(hide_output=True))

    def test_list_fses(self):
        ListFses(LocalExecInfo(hide_output=True))

    def test_fi_info(self):
        FiInfo(LocalExecInfo(hide_output=True))

    def test_blkid(self):
        Blkid(LocalExecInfo(hide_output=True))

    def test_resource_graph(self):
        rg = ResourceGraph()
        rg.build(LocalExecInfo(hide_output=True))
        rg.save('/tmp/resource_graph.yaml')
        rg.load('/tmp/resource_graph.yaml')
        rg.filter_fs(r'/$', '/${USER}', 'NVME')
        rg.filter_hosts(Hostfile(), '1gbps')
        rg.save('/tmp/resource_graph.yaml')

    def test_custom_resource_graph(self):
        rg = ResourceGraph()
        all_hosts = ['host1', 'host2', 'host3']
        all_hosts_ip = ['192.168.1.0', '192.168.1.1', '192.168.1.2']
        providers = ['tcp', 'ib', 'roce']
        hosts = Hostfile(all_hosts=all_hosts, all_hosts_ip=all_hosts_ip)

        # Add networks for each node
        rg.set_hosts(hosts)
        for provider in providers:
            rg.add_net(hosts,
                       provider=provider)
        rg.add_net(hosts.subset(1),
                   provider='uncommon')

        # Add common storage for each node
        rg.add_storage(hosts,
                       device='/dev/sda1',
                       mount='/',
                       tran='sata',
                       rota=True,
                       size=SizeConv.to_int('10g'),
                       shared=False)
        rg.add_storage(hosts,
                       device='/dev/sda2',
                       mount='/mnt/hdd/$USER',
                       tran='sata',
                       rota=True,
                       size=SizeConv.to_int('200g'),
                       shared=False)
        rg.add_storage(hosts,
                       device='/dev/sdb1',
                       mount='/mnt/ssd/$USER',
                       tran='sata',
                       rota=False,
                       size=SizeConv.to_int('50g'),
                       shared=False)
        rg.add_storage(hosts,
                       device='/dev/nvme0n1',
                       mount='/mnt/nvme/$USER',
                       tran='nvme',
                       rota=False,
                       size=SizeConv.to_int('100g'),
                       shared=False)
        rg.add_storage(hosts.subset(1),
                       device='/dev/nvme0n2',
                       mount='/mnt/nvme2/$USER',
                       tran='nvme',
                       rota=False,
                       size=SizeConv.to_int('10g'),
                       shared=False)
        rg.add_storage(hosts,
                       device='/dev/nvme0n3',
                       tran='nvme',
                       rota=False,
                       size=SizeConv.to_int('100g'),
                       shared=False)

        # Filter only mounts in '/mnt'
        rg.filter_fs('/mnt/*')

        # Apply changes
        rg.apply()

        # Find all mounted NVMes
        df = rg.find_storage([StorageDeviceType.NVME])
        self.assertTrue(len(df[df.tran == 'nvme']) == 4)
        self.assertTrue(len(df[df.tran == 'sata']) == 0)
        self.assertTrue(len(df) == 4)

        # Find all mounted & common NVMes and SSDs
        df = rg.find_storage([StorageDeviceType.NVME,
                              StorageDeviceType.SSD],
                             common=True)
        self.assertTrue(len(df[df.tran == 'nvme']) == 3)
        self.assertTrue(len(df[df.tran == 'sata']) == 3)
        self.assertTrue(len(df) == 6)

        # Select a single nvme per-node
        df = rg.find_storage([StorageDeviceType.NVME,
                              StorageDeviceType.SSD],
                             common=True,
                             count_per_node=1)
        self.assertTrue(len(df[df.tran == 'nvme']) == 3)
        self.assertTrue(len(df[df.tran == 'sata']) == 0)
        self.assertTrue(len(df) == 3)

        # Select a single nvme and ssd per-node
        df = rg.find_storage([StorageDeviceType.NVME,
                              StorageDeviceType.SSD],
                             common=True,
                             count_per_dev=1)
        self.assertTrue(len(df[df.tran == 'nvme']) == 3)
        self.assertTrue(len(df[df.tran == 'sata']) == 3)
        self.assertTrue(len(df) == 6)

        # Find common networks between hosts
        df = rg.find_net_info(hosts)
        self.assertTrue(len(df) == 9)

        # Find common tcp networks
        df = rg.find_net_info(hosts, providers='tcp')
        self.assertTrue(len(df) == 3)
