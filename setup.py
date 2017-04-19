from setuptools import setup, find_packages

install_requires = ['psycopg2>=2.7.1',
                    'testing.postgresql']

setup(
    name='luigi_postgres_dburl',
    version='0.0.1',
    package_dir={'luigi_postgres_dburl': 'luigi_postgres_dburl'},
    packages=find_packages(),
    install_requires=install_requires,
    description='Add support for dburl connections to luigi. Based on work in luigi.contrib.postgres',
    author='Henry Rizzi',
    author_email='henry.rizzi@adops.com',
    classifiers=[
        'Development Status :: 5 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries',
        'Topic :: Utilities',
        ],
    url='https://github.com/OAODEV/luigi-postgres-dburl',
)
