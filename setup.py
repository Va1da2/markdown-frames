"""
The setup.py file structure is taken from the https://github.com/CamDavidsonPilon/lifetimes
"""
import os
from setuptools import setup


exec(compile(open('lifetimes/version.py').read(),
             'lifetimes/version.py', 'exec'))


readme_path = os.path.join(os.path.dirname(__file__), 'README.md')

try:
    import pypandoc
    long_description = pypandoc.convert_file(readme_path, 'rst')
except(ImportError):
    long_description = open(readme_path).read()


setup(
    name='Markdown-frames',
    version=__version__,
    description='Convert budiful markdown tables to spark / pandas dataframe for unit tests.',
    author='Vaidas Armonas',
    author_email='vaidas.armonas@gmail.com',
    packages=['markdown_frames'],
    license="MIT",
    keywords="testing pyspark dataframe pandas dataframe unitests",
    url="https://github.com/Va1da2/markdown-frames",
    long_description=long_description,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Topic :: Software Development :: Testing",
        ],
    install_requires=[
        "pyspark>=2.1.1",
        "pandas>=0.19",
        ]
    )