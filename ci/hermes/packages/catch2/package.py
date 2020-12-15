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
#     spack install catch2-git
#
# You can edit this file again by typing:
#
#     spack edit catch2-git
#
# See the Spack documentation for more information on packaging.
# ----------------------------------------------------------------------------

from spack import *


class Catch2(CMakePackage):

    homepage = "https://github.com/catchorg/Catch2"
    url      = "https://github.com/catchorg/Catch2.git"

    version('2.13.3', sha256='fedc5b008f7eb574f45098e7c7138211c543f0f8ad04792090e790511697a877')
    
    depends_on('cmake@3.17.3')
    def url_for_version(self, version):
        url = "https://github.com/catchorg/Catch2/archive/v{0}.tar.gz"
        return url.format(version)

    def cmake_args(self):
        options = ['-DCMAKE_INSTALL_PREFIX={}'.format(self.prefix),
                '-DBUILD_TESTING=OFF']
        return options

    def set_include(self,env,path):
        env.append_flags('CFLAGS', '-I{}'.format(path))
        env.append_flags('CXXFLAGS', '-I{}'.format(path))
    def set_lib(self,env,path):
        env.prepend_path('LD_LIBRARY_PATH', path)
        env.append_flags('LDFLAGS', '-L{}'.format(path))
    def set_flags(self,env):
        self.set_include(env,'{}/include'.format(self.prefix))
        self.set_include(env,'{}/include'.format(self.prefix))
        self.set_lib(env,'{}/lib'.format(self.prefix))
        self.set_lib(env,'{}/lib64'.format(self.prefix))
    def setup_dependent_environment(self, spack_env, run_env, dependent_spec):
        self.set_flags(spack_env)
    def setup_run_environment(self, env):
        self.set_flags(env) 
