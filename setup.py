#!/usr/bin/env python3
from setuptools import setup


with open("requirements.txt") as f:
    requirements = f.readlines()


setup(name='pyods',
      version='0.0.2',
      description='Open Directory Scraper',
      url='https://git.davepedu.com/dave/pyods',
      author='dpedu',
      author_email='dave@davepedu.com',
      packages=['pyods'],
      entry_points={
          'console_scripts': [
              'pyods = pyods.cli:main'
          ]
      },
      install_requires=requirements,
      zip_safe=False)
