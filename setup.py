from setuptools import setup

_desc = """PyGoogalytics allows a user to quickly and simply download Google Analytics and Google Search Console data
in the form of a pandas dataframe."""

setup(
    name='pygoogalytics',
    version='0.1.3',
    description=_desc,
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
        "Intended Audience :: Developers",
        'Development Status :: 1 - Planning',
        'License :: OSI Approved :: MIT License',
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    include_package_data=True,
    package_data={'': ['data/*.csv']},
)

# python setup.py check
# python setup.py sdist
# python setup.py bdist_wheel --universal
# twine upload dist/*
