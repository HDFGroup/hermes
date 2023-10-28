from spack import *

class Hermes(CMakePackage):
    homepage = "http://www.cs.iit.edu/~scs/assets/projects/Hermes/Hermes.html"
    url = "https://github.com/HDFGroup/hermes/tarball/master"
    git = "https://github.com/HDFGroup/hermes.git"

    version('master',
            branch='master', submodules=True)
    version('dev', branch='dev', submodules=True)
    version("1.0.5-beta", sha256="1f3ba51a8beda4bc1314d6541b800de1525f5e233a6f498fcde6dc43562ddcb7")
    version("1.0.0-beta", sha256="301084cced32aa00532ab4bebd638c31b0512c881ffab20bf5da4b7739defac2")
    version("0.9.9-beta", sha256="d2e0025a9bd7a3f05d3ab608c727ed15d86ed30cf582549fe996875daf6cb649")
    version("0.9.8-beta", sha256="68e9a977c25c53dcab7d7f6ef0df96b2ba4a09a06aa7c4a490c67faa2a78f077")
    version("0.9.5-beta", sha256="f48d15591a6596e8e54897362ec2591bc71e5de92933651f4768145e256336ca")
    version("0.9.0-beta", sha256="46e64ae031a9766d08d96f163a021fc1a7be61d90f4a918912231ebf387dd37a")
    version("0.8.0-beta...v0.9.0-beta", sha256="c73fc9d46abf11f1b0ff9ddac49805ab6612a97d4c82f3719e3d8ac92cc8be8b")
    version("0.8.0-beta", sha256="68835bcf3876b83122f1e605ee5b06061a36e83c411120d53e9840a28022a6e9")
    version("0.7.0-beta...v0.8.0-beta", sha256="c6600da4342bfe35d23e8d1d66eb36e99478de4b54db23e94b0a143b39f6a66f")
    version("0.7.0-beta", sha256="44a0974bb9b78fb6d3f9e814e30a418e78f1205b655bddfbb14b58fec3aadb1f")
    version("0.6.0-beta...v0.7.0-beta", sha256="f2ce595daf7cc501d76ed7efc289c4b6e6720147f32722107beed31947b17778")
    version("0.6.0-beta", sha256="a04361db30f25e7d31a31779c18c136ca2e95caee335bfff63959216df3f8a34")
    version("0.5.0-beta...v0.6.0-beta", sha256="8a0e6b7781baac751081f28010da05532c25d0698eeba5194a8bd198453f9212")
    version("0.5.0-beta", sha256="1cb33749544d33ce838ff181faf77ddafc1a93ce71be232dffee1bce89f77f38")
    version("0.4.0-beta...v0.5.0-beta", sha256="f94f9793ddd6595eb541a863d5ab3d030473a57a48c9486dfd32f30705f233c1")
    version("0.4.0-beta", sha256="06020836e203b2f680bea24007dc73760dfb977eb61e442b795b264f0267c16b")
    version("0.3.0-beta...v0.4.0-beta", sha256="7729b115598277adcab019dee24e5276698fb595066bca758bfa59dc8d51c5a4")

    depends_on('hermes_shm')

    # Common across hermes_shm and hermes
    variant('mpiio', default=True, description='Enable MPI I/O adapter')
    variant('stdio', default=True, description='Enable STDIO adapter')
    variant('vfd', default=False, description='Enable HDF5 VFD')
    variant('ares', default=False, description='Enable full libfabric install')
    variant('only_verbs', default=False, description='Only verbs')
    variant('debug', default=False, description='Build shared libraries')
    variant('zmq', default=False, description='Build ZeroMQ tests')

    depends_on('mochi-thallium~cereal@0.10.1')
    depends_on('catch2@3.0.1')
    depends_on('mpi')
    depends_on('cereal')
    depends_on('yaml-cpp')
    depends_on('doxygen@1.9.3')
    depends_on('boost@1.7: +context +fiber +filesystem +system +atomic +chrono +serialization +signals +pic +regex')
    depends_on('libfabric fabrics=sockets,tcp,udp,verbs',
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
        if '+mpiio' in self.spec:
            args.append('-DHERMES_ENABLE_MPIIO_ADAPTER=ON')
            if 'openmpi' in self.spec:
                args.append('-DHERMES_OPENMPI=ON')
            elif 'mpich' in self.spec:
                args.append('-DHERMES_MPICH=ON')
        if '+stdio' in self.spec:
            args.append('-HERMES_ENABLE_STDIO_ADAPTER=ON')
        if '+vfd' in self.spec:
            args.append('-HERMES_ENABLE_VFD=ON')
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
