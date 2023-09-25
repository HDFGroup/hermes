from spack import *

class Hermes(CMakePackage):
    homepage = "http://www.cs.iit.edu/~scs/assets/projects/Hermes/Hermes.html"
    url = "https://github.com/HDFGroup/hermes/tarball/master"
    git = "https://github.com/HDFGroup/hermes.git"

    version('master', branch='master')
    version('pnnl', branch='pnnl')
    version('dev-priv', git='https://github.com/lukemartinlogan/hermes.git', branch='dev')
    version('dev-1.1', git='https://github.com/lukemartinlogan/hermes.git', branch='hermes-1.1')
    version('hdf-1.1', git='https://github.com/HDFGroup/hermes.git', branch='hermes-1.1')

    variant('vfd', default=False, description='Enable HDF5 VFD')
    variant('ares', default=False, description='Enable full libfabric install')
    variant('debug', default=False, description='Enable debug mode')
    variant('zmq', default=False, description='Build ZeroMQ tests')

    depends_on('mochi-thallium~cereal@0.10.1')
    depends_on('cereal')
    # depends_on('catch2@3.0.1')
    depends_on('catch2')
    depends_on('mpich@3.3.2')
    depends_on('yaml-cpp')
    depends_on('boost@1.7:')
    depends_on('hermes_shm')
    depends_on('libzmq', '+zmq')

    def cmake_args(self):
        args = ['-DCMAKE_INSTALL_PREFIX={}'.format(self.prefix)]
        if '+debug' in self.spec:
            args.append('-DCMAKE_BUILD_TYPE=Debug')
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
