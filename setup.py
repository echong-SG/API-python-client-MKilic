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
    keywords="api graphql protocol rest relay gql client safegraph.com safegraph-api",
    packages=find_packages(include=['safegraphql*']),
    install_requires=[
        "pandas==1.3.2",
        "gql==3.0.0b0",
        "aiohttp>=3.7.1,<3.8.0",
    ],
    # dependency_links=[
    #     # Make sure to include the `#egg` portion so the `install_requires` recognizes the package
    #     'git+ssh://git@github.com/graphql-python/gql.git'
    # ],
    setup_requires=['pytest-runner', "wheel"],
    tests_require=['pytest==4.4.1'],
    test_suite='tests',
    platforms="any",
)
