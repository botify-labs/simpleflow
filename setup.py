import os

from setuptools import setup, find_packages

#avoid errors on python setup.py test
#(cf. http://bugs.python.org/issue15881#msg170215)
import multiprocessing

root = os.path.abspath(os.path.dirname(__file__))
version = __import__('cdf').__version__

with open(os.path.join(root, 'README.rst')) as f:
    README = f.read()


DEP_INSTALL = 'install'
DEP_TEST = 'tests'

DEPENDENCY_TYPES = {
    'cpython': {
        DEP_INSTALL: 'python',
        DEP_TEST: 'python_test',
    },
    'pypy': {
        DEP_INSTALL: 'pypy',
        DEP_TEST: 'pypy_test',
    },
}


def load_deps(dep_type, root_path=root):
    import platform

    impl = platform.python_implementation().lower()
    label = DEPENDENCY_TYPES[impl].get(dep_type)
    if label is None:
        raise ValueError('invalid dependency type {}'.format(dep_type))

    path = os.path.join(root_path, 'packaging', label + '.deps')
    with open(path) as f:
        return [s.strip() for s in f]


setup(
    name='botify-cdf',
    version=version,
    license='MIT',

    description='Data extractor from pocket-crawler\'s raw files',
    long_description=README + '\n\n',

    author='ampelmann',
    author_email='thomas@botify.com',
    url='http://github.com/sem-io/botify-cdf',
    keywords='botify data extractor crawl',
    zip_safe=True,
    install_requires=load_deps(DEP_INSTALL),
    tests_require=load_deps(DEP_TEST),
    package_dir={'': '.'},
    include_package_data=False,

    packages=find_packages(),

    test_suite="nose.collector",
)
