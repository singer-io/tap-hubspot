#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path

setup(name='stream-hubspot',
      version='0.1.0',
      description='Streams Hubspot data',
      author='Stitch',
      url='https://github.com/stitchstreams/stream-hubspot',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['stream_hubspot'],
      install_requires=[
          'stitchstream-python>=0.4.1',
          'requests==2.12.4',
          'backoff==1.3.2',
          'python-dateutil==2.6.0',
      ],
      entry_points='''
          [console_scripts]
          stream-hubspot=stream_hubspot:main
      ''',
      packages=['stream_hubspot'],
      package_data = {
          'stream_hubspot': [
              "campaigns.json",
              "companies.json",
              "contact_lists.json",
              "contacts.json",
              "deals.json",
              "email_events.json",
              "forms.json",
              "keywords.json",
              "owners.json",
              "subscription_changes.json",
              "workflows.json",
          ]
      }
)
