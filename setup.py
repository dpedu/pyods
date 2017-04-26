#!/usr/bin/env python3
from setuptools import setup

setup(name='pyods',
      version='0.0.1',
      description='Open Directory Scraper',
      url='http://gitlab.xmopx.net/dave/pyods',
      author='dpedu',
      author_email='dave@davepedu.com',
      packages=['pyods'],
      entry_points={
          'console_scripts': [
              'pyods = pyods.cli:main'
          ]
      },
      zip_safe=False)
