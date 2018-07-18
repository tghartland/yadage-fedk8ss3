import os
from setuptools import setup, find_packages

setup(
  name = 'yadage-extresult',
  version = '0.0.1',
  description = 'yadage backend with external job backend as well as remote result store',
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
