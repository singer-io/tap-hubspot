#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path


with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'VERSION')) as f:
    version = f.read().strip()


setup(name='tap-hubspot',
      version=version,
      description='Taps Hubspot data',
      author='Stitch',
      url='https://github.com/stitchstreams/tap-hubspot',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_hubspot'],
      install_requires=[
          'stitchstream-python>=0.5.0',
          'requests==2.12.4',
          'backoff==1.3.2',
          'python-dateutil==2.6.0',
      ],
      entry_points='''
          [console_scripts]
          tap-hubspot=tap_hubspot:main
      ''',
      packages=['stream_hubspot'],
      package_data = {
          'schemas': [
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
          '': [
              'LICENSE',
              'VERSION',
          ],
      }
)
