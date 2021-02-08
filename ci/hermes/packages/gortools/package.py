# Copyright 2013-2020 Lawrence Livermore National Security, LLC and other
# Spack Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: (Apache-2.0 OR MIT)

# ----------------------------------------------------------------------------
# If you submit this package back to Spack as a pull request,
# please first remove this boilerplate and all FIXME comments.
#
# This is a template package file for Spack.  We've put "FIXME"
# next to all the things you'll want to change. Once you've handled
# them, you can save this file and test your package like this:
#
#     spack install rpclib
#
# You can edit this file again by typing:
#
#     spack edit rpclib
#
# See the Spack documentation for more information on packaging.
# ----------------------------------------------------------------------------

from spack import *


class Gortools(Package):
    homepage = "https://developers.google.com/optimization/install/cpp"
    version('8.0', sha256='ac01d7ebde157daaeb0e21ce54923a48e4f1d21faebd0b08a54979f150f909ee')
    version('7.1', sha256='c4c65cb3351554d207d5bb212601ca4e9d352563cda35a283d75963739d16bbd')
    version('7.2', sha256='13a4de5dba1f64e2e490394f8f63fe0a301ee55466ef65fe309ffd5100358ea8')
    version('7.3', sha256='b87922b75bbcce9b2ab5da0221751a3c8c0bff54b2a1eafa951dbf70722a640e')
    version('7.4', sha256='89fafb63308b012d56a6bb9b8da9dead4078755f137a4f6b3567b36a7f3ba85c')
    version('7.5', sha256='fb111aaacaf89d395d62e9e871833afdcf7f168b9f004bcfc9b70a894852f808')
    version('7.6', sha256='a41202ebe24e030dccaf15846bd24224eec692b523edd191596b6a15159a2d47')
    version('7.7', sha256='d20eb031ea3f1b75e55e44ae6acdb674ee6fb31e7a56498be32dc83ba9bc13bd')
    version('7.8', sha256='d93a9502b18af51902abd130ff5f23768fcf47e266e6d1f34b3586387aa2de68')
    version('7.0', sha256='379c13c9a5ae70bf0e876763005b2d2d51fcf966882b28b1a65344f2d3d2c589')
    depends_on('gflags')
    depends_on('cmake@3.0.2:', '@7.7:')

    def url_for_version(self, version):
        url = "https://github.com/google/or-tools/archive/v{}.tar.gz"
        return url.format(version)

    def install(self, spec, prefix):
        options = ['prefix=%s' % prefix]
        # with working_dir('spack-build', create=True):
        make('third_party')
        make('cc')
        make('install_cc', *options)

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
