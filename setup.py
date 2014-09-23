import os.path
from sys import version_info

from setuptools import setup, find_packages, Command

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.txt')).read()
CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()


_requires_framework = [
    'Flask',
    'Flask-Restless',
    'ofs',
]
requires = \
    _requires_framework


extras_require = {
}


setup(
    name='tagstore',
    version='0.1',
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
    url='',
    keywords='web wsgi',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    test_suite='tests',
    install_requires=requires,
    extras_require=extras_require,
    entry_points = {
    }
)
