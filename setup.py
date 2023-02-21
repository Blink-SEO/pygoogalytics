from setuptools import setup

setup(
    name='pygoogalytics',
    version='0.1.0',
    description='',
    url='https://github.com/Blink-SEO/pygoogalytics',
    author='Joshua Prettyman',
    author_email='joshua@blinkseo.co.uk',
    license='MIT',
    packages=['pygoogalytics'],
    install_requires=[
        'pandas',
        'google-api-python-client',
        'google-analytics-data'
    ],
    classifiers=[
        'Development Status :: 1 - Planning',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)

# python setup.py check
# python setup.py sdist
# twine upload dist/*
