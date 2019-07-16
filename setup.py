__version__ = "0.1.7"

import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='atasker',
    version=__version__,
    author='Altertech Group',
    author_email='div@altertech.com',
    description=
    'Python library for modern thread polling and ask processing using asyncio',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/alttch/atasker',
    packages=setuptools.find_packages(),
    license='Apache License 2.0',
    install_requires=[],
    classifiers=(
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Software Development :: Libraries',
    ),
)
