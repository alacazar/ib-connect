from setuptools import setup, find_packages

setup(
    name='ib_connect',
    version='0.2.0',
    packages=find_packages(),           # ← usually enough now
    install_requires=[
        'ib_insync',
        'pandas',
        'psycopg2-binary',
        'watchdog',
        'flask',
    ],
    entry_points={
        'console_scripts': [
            'ib-query = ib_connect.skills.ib_query.ib_query:main',
            'ib-download = ib_connect.skills.ib_download.ib_download:main',
        ],
    },
)