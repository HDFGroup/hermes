from spack import *

class Hermes(CMakePackage):
    homepage = "http://www.cs.iit.edu/~scs/assets/projects/Hermes/Hermes.html"
    url = "https://github.com/HDFGroup/hermes/tarball/master"
    git = "https://github.com/HDFGroup/hermes.git"
    version('master', branch='master')
    version('dev-priv', git='git@github.com:lukemartinlogan/hermes.git', branch='new-borg')
    variant('vfd', default=False, description='Enable HDF5 VFD')
    depends_on('mochi-thallium~cereal@0.8.3')
    depends_on('catch2@3.0.1')
    depends_on('glpk@4:')
    depends_on('mpich@3.3.2:')
    depends_on('glog@0.4:')
    depends_on('yaml-cpp')
    depends_on('boost@1.7:')
    depends_on('hdf5@1.13.0:', when='+vfd')

    def cmake_args(self):
        args = ['-DCMAKE_INSTALL_PREFIX={}'.format(self.prefix),
                '-DHERMES_RPC_THALLIUM=ON',
                '-DHERMES_INSTALL_TESTS=ON',
                '-DBUILD_TESTING=ON']
        if '+vfd' in self.spec:
            args.append(self.define('HERMES_ENABLE_VFD', 'ON'))
        return args

    def set_include(self, env, path):
        env.append_flags('CFLAGS', '-I{}'.format(path))
        env.append_flags('CXXFLAGS', '-I{}'.format(path))

    def set_lib(self, env, path):
        env.prepend_path('LD_LIBRARY_PATH', path)
        env.append_flags('LDFLAGS', '-L{}'.format(path))

    def set_flags(self, env):
        self.set_include(env, '{}/include'.format(self.prefix))
        self.set_include(env, '{}/include'.format(self.prefix))
        self.set_lib(env, '{}/lib'.format(self.prefix))
        self.set_lib(env, '{}/lib64'.format(self.prefix))

    def setup_dependent_environment(self, spack_env, run_env, dependent_spec):
        self.set_flags(spack_env)

    def setup_run_environment(self, env):
        self.set_flags(env)
