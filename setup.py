from setuptools import find_packages, setup

setup(
    name='safegraph_ql',
    packages=find_packages(include=['safegraph_ql']),
    version='0.1.0',
    description='SafeGraph API Python library',
    author='mirkan1',
    license='MIT',
    install_requires=[],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==4.4.1'],
    test_suite='tests',
)