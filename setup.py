from setuptools import setup, find_packages
import os

setup(
    name='beandispenser',
    version='0.1',
    license = "MIT",
    author = "Willem van der Jagt",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[ 'beanstalkc', 'PyYAML' ],
    data_files=[('/etc/init', ['init_script/beandispenser.conf']),
                ('/etc', ['conf/beandispenser.yml'])],
    entry_points='''
        [console_scripts]
        beandispenser=beandispenser.beandispenser:main
    '''
)