#!/usr/bin/env python

from setuptools import setup

setup(name='tap-googleplay',
      version='0.0.1',
      description='Singer.io tap for downloading reports from the Google Play Console',
      author='JustEdro',
      url='https://github.com/JustEdro',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap-googleplay'],
      install_requires=[
          'singer-python==5.2.3',
          'google-api-python-client==1.7.9',
          'google-cloud-storage==1.16.1',
          'pytz'
      ],
      entry_points='''
          [console_scripts]
          tap-googleplay=tap_googleplay:main
      ''',
      packages=['tap_googleplay'],
      package_data={
          'tap_googleplay/schemas': [
              'installs.json'
          ],
      },
      include_package_data=True,
      )
