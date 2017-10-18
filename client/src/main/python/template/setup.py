from setuptools import setup

setup(
    name='timely',
    version='${python.client.version}',
    packages=['timely'],
    install_requires=[
        'seaborn',
	'plotly'
    ],
    license='Apache License, Version 2.0'
)
