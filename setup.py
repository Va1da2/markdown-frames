"""Project configuration."""

import os
import sys
from setuptools import find_packages
from setuptools import setup
from markdown_frames import __version__

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

install_requires = [
    'pyspark>=2.1.1',
    'pandas',
]
if sys.version_info < (3, 5,):
    install_requires.append('scandir')
    install_requires.append('typing')

version = __version__

setup(
    name='markdown_frames',
    version=version,
    packages=find_packages(exclude=('tests', 'docs',)),

    author='Vaidas Armonas',
    author_email='vaidas.armonas@gmail.com',
    url='https://github.com/Va1da2/markdown-frames',
    description='Markdown tables parsing to pyspark / pandas dataframes.',

    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Topic :: Software Development :: Testing",
    ],

    install_requires=install_requires,
    extras_require={
        'dev': [
            'mccabe>=0.5.0',
            'pylint',
            'pytest',
            'pytest-flake8',
            'pytest-coverage',
            'flake8',
            'Sphinx',
        ]
    }
)