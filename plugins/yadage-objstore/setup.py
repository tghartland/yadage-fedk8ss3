import os
from setuptools import setup, find_packages

setup(
  name = 'yadage-objstore',
  version = '0.0.1',
  description = 'A yadage state plugin for sharing workflow data via object stores',
  url = '',
  author = 'Lukas Heinrich',
  author_email = 'lukas.heinrich@cern.ch',
  packages = find_packages(),
  include_package_data = True,
  install_requires = ['yadage','minio','packtivity'],
  entry_points = {
  },
  dependency_links = [
  ]
)
