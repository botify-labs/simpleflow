import os

from setuptools import setup, find_packages

#avoid errors on python setup.py test
#(cf. http://bugs.python.org/issue15881#msg170215)
import multiprocessing

root = os.path.abspath(os.path.dirname(__file__))
version = __import__('cdf').__version__

with open(os.path.join(root, 'README.rst')) as f:
    README = f.read()


def load_deps(path, root_path=root):
    with open(os.path.join(root_path, path)) as f:
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
    install_requires=load_deps('packaging/python.deps'),
    tests_require=load_deps('packaging/python_test.deps'),
    data_files=[
        (
        "cdf/features/rel/data/",
        ["cdf/features/rel/data/ISO-3166-1-alpha-2.json"]
        )
    ],
    package_dir={'': '.'},
    include_package_data=False,

    packages=find_packages(),

    test_suite="nose.collector",
)
