import os

from setuptools import setup, find_packages


root = os.path.abspath(os.path.dirname(__file__))
version = __import__('cdf').__version__

with open(os.path.join(root, 'README.rst')) as f:
    README = f.read()

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
    install_requires=[
        'boto==2.9.8',
        'ujson==1.33',
        'pyhash==0.5.0',
        'pyelasticsearch==0.5',
        'lz4==0.6.0',
        'numpy',
        'pandas==0.11.0',
        'numexpr==2.1',
        'Cython==0.19.1',
        'tables==3.0.0',
        'lockfile==0.9.1'
    ],

    package_dir={'': '.'},
    include_package_data=False,

    packages=find_packages(),
)
