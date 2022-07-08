import os
from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as f:
    readme = f.read()

setup(
    name='Nanodis',
    version=__import__('Nanodis').__version__,
    description='A Python miniature Redis-like db',
    author='Gustavo Cavalcante',
    author_email='nowayhecodes@gmail.com',
    url='https://github.com/nowayhecodes/nanodis',
    packages=[],
    py_modules=['nanodis'],
    classifiers=[
        'Development Status :: Freeze',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    scripts=['nanodis/__init__.py'],
    test_suite='tests'
)
