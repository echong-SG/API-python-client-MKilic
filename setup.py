from setuptools import find_packages, setup
import os

DIRECTORY = os.path.dirname(__file__)

#REQUIREMENTS = open(os.path.join(DIRECTORY, "REQUIREMENTS.txt")).read().split()
VERSION = open(os.path.join(DIRECTORY, "safegraphql", "__version__.py")).read()
READ_ME = open(os.path.join(DIRECTORY, "README.md")).read()

setup(
    name='safegraphQL',
    version=VERSION,
    description='graphQL API of safegraph.com using Python functions',
    long_description=READ_ME,
    long_description_content_type="text/markdown",
    url="https://github.com/echong-SG/API-python-client-MKilic",
    author="Renas Mirkan Kilic",
    author_email="mirkanbaba1@gmail.com",
    license='MIT',
    classifiers=[
        "Intended Audience :: Science/Research",
        "Topic :: Software Development :: Libraries",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    keywords="api graphql safegraph gql safegraph-api",
    packages=find_packages(include=['safegraphql']),
    install_requires=[
        # "graphql-core>=2.3.2",
        # "yarl>=1.6,<2.0",
        "gql>=2.0.0",
        "pandas>=1.3.2"
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==4.4.1'],
    test_suite='tests',
    platforms="any",
)
