from jarvis_util.util.hostfile import Hostfile
import pathlib


def test1():
    host = Hostfile()
    host.parse('0')
    assert(len(host.hosts) == 1)
    assert(host.hosts[0] == '0')


def test2():
    host = Hostfile()
    host.parse('ares-comp-01')
    assert(len(host.hosts) == 1)
    assert(host.hosts[0] == 'ares-comp-01')


def test3():
    host = Hostfile()
    host.parse('ares-comp-[01-04]-40g')
    assert(len(host.hosts) == 4)
    assert(host.hosts[0] == 'ares-comp-01-40g')
    assert(host.hosts[1] == 'ares-comp-02-40g')
    assert(host.hosts[2] == 'ares-comp-03-40g')
    assert(host.hosts[3] == 'ares-comp-04-40g')


def test4():
    host = Hostfile()
    host.parse('ares-comp-[01-02]-40g-[01-02]')
    assert(len(host.hosts) == 4)
    assert(host.hosts[0] == 'ares-comp-01-40g-01')
    assert(host.hosts[1] == 'ares-comp-01-40g-02')
    assert(host.hosts[2] == 'ares-comp-02-40g-01')
    assert(host.hosts[3] == 'ares-comp-02-40g-02')


def test5():
    host = Hostfile()
    host.parse('ares-comp-[01-02]-40g-[01-02]')
    host = host.subset(3)
    assert(len(host.hosts) == 3)
    assert(host.is_subset())
    assert(host.hosts[0] == 'ares-comp-01-40g-01')
    assert(host.hosts[1] == 'ares-comp-01-40g-02')
    assert(host.hosts[2] == 'ares-comp-02-40g-01')


def test6():
    HERE = str(pathlib.Path(__file__).parent.resolve())
    # host = Hostfile(hostfile=f"{HERE}/test_hostfile.txt")
    # print(host.hosts)
    # assert(len(host.hosts) == 15)


test1()
test2()
test3()
test4()
test5()
test6()
