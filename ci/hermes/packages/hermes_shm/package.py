from spack import *

class HermesShm(CMakePackage):
    homepage = "https://github.com/lukemartinlogan/hermes_shm/wiki"
    git = "https://github.com/lukemartinlogan/hermes_shm.git"
    version('master', branch='master')
    depends_on('mochi-thallium~cereal@0.10.1')
    depends_on('catch2@3.0.1')
    depends_on('mpi')
    depends_on('boost@1.7:')
    depends_on('cereal')
    depends_on('doxygen@1.9.3')

    variant('debug', default=False, description='Build shared libraries')

    def cmake_args(self):
        args = []
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
