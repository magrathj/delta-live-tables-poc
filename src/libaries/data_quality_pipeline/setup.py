from setuptools import setup, find_packages

VERSION = '0.0.1' 
DESCRIPTION = 'DQ Pipeline'
LONG_DESCRIPTION = 'Data Quality Pipeline using DLT framework'

# Setting up
setup(
       # the name must match the folder name 'verysimplemodule'
        name="data_quality_pipeline", 
        version=VERSION,
        author="jared magrath",
        author_email="magrathj@tcd.ie",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=[]
)