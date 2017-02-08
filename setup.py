#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path


setup(name='tap-hubspot',
      version='0.2.0',
      description='Taps Hubspot data',
      author='Stitch',
      url='https://github.com/stitchstreams/tap-hubspot',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_hubspot'],
      install_requires=[
          'stitchstream-python>=0.6.0',
          'requests==2.12.4',
          'backoff==1.3.2',
          'python-dateutil==2.6.0',
      ],
      entry_points='''
          [console_scripts]
          tap-hubspot=tap_hubspot:main
      ''',
      packages=['tap_hubspot'],
      package_data = {
          'tap_hubspot': [
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
          ],
      }
)
