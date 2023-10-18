from spack import *

class HermesShm(CMakePackage):
    homepage = "https://github.com/lukemartinlogan/hermes_shm/wiki"
    git = "https://github.com/lukemartinlogan/hermes_shm.git"
    url = "https://github.com/lukemartinlogan/hermes_shm/archive/refs/tags/v1.0.0.tar.gz"

    version('master', branch='master')
    version("1.0.0", sha256="a79f01d531ce89985ad59a2f62b41d74c2385e48d929e2f4ad895ae34137573b")

    variant('ares', default=False, description='Enable full libfabric install')
    variant('only_verbs', default=False, description='Only verbs')
    variant('vfd', default=False, description='Enable HDF5 VFD')
    variant('debug', default=False, description='Build shared libraries')
    variant('zmq', default=False, description='Build ZeroMQ tests')

    depends_on('mochi-thallium~cereal@0.10.1')
    depends_on('catch2@3.0.1')
    depends_on('mpich@3.3.2')
    depends_on('cereal')
    depends_on('yaml-cpp')
    depends_on('doxygen@1.9.3')
    depends_on('boost@1.7: +context +fiber +filesystem +system +atomic +chrono +serialization +signals +pic')
    depends_on('libfabric fabrics=sockets,tcp,udp,rxm,rxd,verbs',
               when='+ares')
    depends_on('libfabric fabrics=verbs',
               when='+only_verbs')
    depends_on('libzmq', '+zmq')
    depends_on('hdf5@1.14.0', when='+vfd')

    def cmake_args(self):
        args = []
        if '+debug' in self.spec:
            args.append('-DCMAKE_BUILD_TYPE=Debug')
        else:
            args.append('-DCMAKE_BUILD_TYPE=Release')
        return args

    def set_include(self, env, path):
        env.append_flags('CFLAGS', '-I{}'.format(path))
        env.append_flags('CXXFLAGS', '-I{}'.format(path))
        env.prepend_path('INCLUDE', '{}'.format(path))
        env.prepend_path('CPATH', '{}'.format(path))

    def set_lib(self, env, path):
        env.prepend_path('LIBRARY_PATH', path)
        env.prepend_path('LD_LIBRARY_PATH', path)
        env.append_flags('LDFLAGS', '-L{}'.format(path))

    def set_flags(self, env):
        self.set_include(env, '{}/include'.format(self.prefix))
        self.set_include(env, '{}/include'.format(self.prefix))
        self.set_lib(env, '{}/lib'.format(self.prefix))
        self.set_lib(env, '{}/lib64'.format(self.prefix))
        env.prepend_path('CMAKE_PREFIX_PATH', '{}/cmake'.format(self.prefix))

    def setup_dependent_environment(self, spack_env, run_env, dependent_spec):
        self.set_flags(spack_env)

    def setup_run_environment(self, env):
        self.set_flags(env)
