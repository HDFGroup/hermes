from jarvis_util.util.hostfile import Hostfile
import pathlib
from unittest import TestCase


class TestHostfile(TestCase):
    def test_no_expand_int(self):
        host = Hostfile(text='0', find_ips=False)
        self.assertTrue(len(host.hosts) == 1)
        self.assertTrue(host.hosts[0] == '0')

    def test_no_expand(self):
        host = Hostfile(text='ares-comp-01', find_ips=False)
        self.assertTrue(len(host.hosts) == 1)
        self.assertTrue(host.hosts[0] == 'ares-comp-01')

    def test_expand_set(self):
        host = Hostfile(text='ares-comp-[01-04]-40g', find_ips=False)
        self.assertTrue(len(host.hosts) == 4)
        self.assertTrue(host.hosts[0] == 'ares-comp-01-40g')
        self.assertTrue(host.hosts[1] == 'ares-comp-02-40g')
        self.assertTrue(host.hosts[2] == 'ares-comp-03-40g')
        self.assertTrue(host.hosts[3] == 'ares-comp-04-40g')

    def test_expand_two_sets(self):
        host = Hostfile(text='ares-comp-[01-02]-40g-[01-02]', find_ips=False)
        self.assertTrue(len(host.hosts) == 4)
        self.assertTrue(host.hosts[0] == 'ares-comp-01-40g-01')
        self.assertTrue(host.hosts[1] == 'ares-comp-01-40g-02')
        self.assertTrue(host.hosts[2] == 'ares-comp-02-40g-01')
        self.assertTrue(host.hosts[3] == 'ares-comp-02-40g-02')

    def test_subset(self):
        host = Hostfile(text='ares-comp-[01-02]-40g-[01-02]', find_ips=False)
        host = host.subset(3)
        self.assertTrue(len(host.hosts) == 3)
        self.assertTrue(host.is_subset())
        self.assertTrue(host.hosts[0] == 'ares-comp-01-40g-01')
        self.assertTrue(host.hosts[1] == 'ares-comp-01-40g-02')
        self.assertTrue(host.hosts[2] == 'ares-comp-02-40g-01')

    def test_read_hostfile(self):
        HERE = str(pathlib.Path(__file__).parent.resolve())
        hf = Hostfile(hostfile=f'{HERE}/test_hostfile.txt', find_ips=False)
        print(hf.hosts)
        self.assertEqual(len(hf), 15)

    def test_save_hostfile(self):
        HERE = str(pathlib.Path(__file__).parent.resolve())
        hf = Hostfile(hostfile=f'{HERE}/test_hostfile.txt', find_ips=False)
        hf_sub = hf.subset(4)
        self.assertEqual(len(hf_sub), 4)
        hf_sub.save('/tmp/test_hostfile.txt')
        hf_sub_reload = Hostfile(hostfile=f'/tmp/test_hostfile.txt',
                                 find_ips=False)
        self.assertEqual(len(hf_sub_reload), 4)
        self.assertEqual(hf_sub, hf_sub_reload)
