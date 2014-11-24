import os.path
from sys import version_info

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.txt')).read()
CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()


_requires_framework = [
    'Flask',
    'Flask-Restless',
    'ofs',
    'tempfilezipstream>=2.0',
]
requires = \
    _requires_framework


setup(
    name='tagstore',
    version='1.0',
    description='tagstore',
    long_description=README + '\n\n' +  CHANGES,
    classifiers=[
        "Programming Language :: Python",
        "Framework :: Flask",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
    ],
    author='CCHDO',
    author_email='cchdo@ucsd.edu',
    url='https://bitbucket.org/ghdc/tagstore',
    keywords='web wsgi',
    include_package_data=True,
    packages=find_packages(),
    test_suite='tests',
    install_requires=requires,
)
