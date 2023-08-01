from setuptools import setup
from Cython.Build import cythonize

setup(
    ext_modules=cythonize(r"./.scripts/gtfs_static_utils.pyx",
    compiler_directives = { "language_level" : "3str"}),
)