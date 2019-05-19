# -*- coding: utf-8 -*-
#pylint: skip-file

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='streamric',
    version='0.0.1',
    description='Log based alerting',
    long_description=readme,
    author='Johan van den Dorpe',
    author_email='johan@johan.org.uk',
    url='https://github.com/johanek/streamric.git',
    license=license,
    packages=find_packages(exclude=('tests', 'docs')),
    scripts=['bin/streamric'],
    install_requires=[
        'confluent-kafka',
        'ujson',
        'voluptuous',
        'pyyaml',
        'schedule',
        'redis'
    ],
    setup_requires=[
        'pytest-runner',
        'pytest-flakes',
    ],
    tests_require=[
        'pytest',
        'pyflakes',
        'pytest-mock'
    ],
)
