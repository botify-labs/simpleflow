import os

from setuptools import setup, find_packages

#avoid errors on python setup.py test
#(cf. http://bugs.python.org/issue15881#msg170215)
import multiprocessing

root = os.path.abspath(os.path.dirname(__file__))
version = __import__('cdf').__version__

with open(os.path.join(root, 'README.rst')) as f:
    README = f.read()

with open(os.path.join(root, 'packaging/python.deps')) as f:
    install_requirements = [s.strip() for s in f]

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
    install_requires=install_requirements,
    tests_require=[
        'mock',
        'nose',
        'httpretty==0.7.0',
        'moto'
    ],

    package_dir={'': '.'},
    include_package_data=False,

    packages=find_packages(),

    test_suite="nose.collector",
)
