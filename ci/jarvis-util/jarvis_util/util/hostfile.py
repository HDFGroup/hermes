import os
import socket
import re
import itertools
import copy


class Hostfile:
    def __init__(self, hostfile=None, all_hosts=None):
        """
        Constructor. Parse hostfile or store existing host list.

        :param hostfile: The path to the hostfile
        :param all_hosts: a list of strings representing all hosts
        """
        self.hosts_ip = []
        self.hosts = []
        self.all_hosts = []
        self.all_hosts_ip = []
        self.path = hostfile

        # Direct constructor
        if all_hosts is not None:
            self._set_hosts(all_hosts)

        # Hostfile constructor
        elif hostfile is not None:
            self._load_hostfile(self.path)

    def parse(self, text, set_hosts=False):
        """
        Parse a line of a hostfile. Used mainly for unit tests.

        :param text: A line of the hostfile
        :param set_hosts: Whether or not to set hosts
        :return:
        """

        hosts = []
        self._expand_line(hosts, text)
        if set_hosts:
            self._set_hosts(hosts)
        else:
            self.hosts = hosts

    def _load_hostfile(self, path):
        """
        Expand a hostfile

        :param path: the path to the hostfile
        :return:
        """
        if not os.path.exists(path):
            raise Exception("hostfile not found")
        hosts = []
        with open(path, 'r') as fp:
            lines = fp.read().splitlines()
            for line in lines:
                self._expand_line(hosts, line)
        self.path = path
        self._set_hosts(hosts)
        return self

    def _expand_line(self, hosts, line):
        """
        Will expand brackets in a host declaration.
        E.g., host-[0-5,...]-name

        :param hosts: the current set of hosts
        :param line: the line to parse
        :return: None
        """
        toks = re.split('[\[\]]', line)
        brkts = [tok for i, tok in enumerate(toks) if i % 2 == 1]
        num_set = []

        # Get the expanded set of numbers for each bracket
        for i, brkt in enumerate(brkts):
            num_set.append([])
            self._expand_set(num_set[-1], brkt)

        # Expand the host string
        host_nums = self._product(num_set)
        for host_num in host_nums:
            host = []
            for i, tok in enumerate(toks):
                if i % 2 == 1:
                    host.append(host_num[int(i/2)])
                else:
                    host.append(tok)
            hosts.append("".join(host))

    def _expand_set(self, num_set, brkt):
        """
        Expand a bracket set.
        The bracket initially has notation: [0-5,0-9,...]
        """

        rngs = brkt.split(',')
        for rng in rngs:
            self._expand_range(num_set, rng)

    def _expand_range(self, num_set, brkt):
        """
        Expand a range.
        The range has notation: A-B or A

        :param num_set: the numbers in the range
        :param brkt:
        :return:
        """
        if len(brkt) == 0:
            return
        if '-' in brkt:
            min_max = brkt.split('-')
            if len(min_max[0]) == len(min_max[1]):
                nums = range(int(min_max[0]), int(min_max[1]) + 1)
                num_set += [str(num).zfill(len(min_max[0])) for num in nums]
            else:
                nums = range(int(min_max[0]), int(min_max[1]) + 1)
                num_set += [str(num) for num in nums]
        else:
            num_set.append(brkt)

    def _product(self, num_set):
        """
        Return the cartesian product of the number set

        :param num_set: The numbers to product
        :return:
        """
        return [element for element in itertools.product(*num_set)]

    def _set_hosts(self, all_hosts):
        self.all_hosts = all_hosts
        self.all_hosts_ip = [socket.gethostbyname(host) for host in all_hosts]
        self.hosts = self.all_hosts
        self.hosts_ip = self.all_hosts_ip
        return self

    def subset(self, count):
        sub = Hostfile()
        sub.path = self.path
        sub.all_hosts = self.all_hosts
        sub.all_hosts_ip = self.all_hosts_ip
        sub.hosts = self.hosts[:count]
        sub.hosts_ip = self.hosts_ip[:count]
        return sub

    def is_subset(self):
        return len(self.hosts) != len(self.all_hosts)

    def path(self):
        return self.path

    def save(self, path):
        self.all_hosts = self.hosts
        self.all_hosts_ip = self.hosts_ip
        self.path = path
        with open(path, 'w') as fp:
            fp.write("\n".join(self.all_hosts))
        return self

    def ip_list(self):
        return self.hosts_ip

    def hostname_list(self):
        return self.hosts

    def enumerate(self):
        return enumerate(self.hosts)

    def host_str(self, sep=','):
        return sep.join(self.hosts)

    def ip_str(self, sep=','):
        return sep.join(self.hosts_ip)

    def __len__(self):
        return len(self.hosts)

    def __getitem__(self, idx):
        return self.hosts[idx]

    def __str__(self):
        return str(self.hosts)

    def __repr__(self):
        return str(self)
