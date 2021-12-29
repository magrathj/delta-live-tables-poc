from setuptools import setup, find_packages

VERSION = '0.0.1' 
DESCRIPTION = 'DLT wrapper'
LONG_DESCRIPTION = 'DLT wrapper to simulate local development'

# Setting up
setup(
       # the name must match the folder name 'verysimplemodule'
        name="dlt", 
        version=VERSION,
        author="jared magrath",
        author_email="magrathj@tcd.ie",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=[]
)