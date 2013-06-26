import os

from setuptools import setup

root = os.path.abspath(os.path.dirname(__file__))

version = __import__('cdf').__version__

with open(os.path.join(root, 'README.rst')) as f:
    README = f.read()

setup(
    name='cdf',
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
        'ujson==1.33',
        'pyhash=0.5.0',
    ],

    package_dir={'': '.'},
    include_package_data=False,

    packages=[
        'cdf',
    ],
)
