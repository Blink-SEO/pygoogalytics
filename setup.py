from setuptools import setup

# python setup.py check
# python setup.py sdist
# python setup.py bdist_wheel --universal
# twine upload dist/*

_desc = """PyGoogalytics allows a user to quickly and simply download Google Analytics and Google Search Console data
in the form of a pandas dataframe."""

setup(
    name='pygoogalytics',
    version='0.3.0',
    description=_desc,
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/Blink-SEO/pygoogalytics',
    author='Joshua Prettyman',
    author_email='joshua@blinkseo.co.uk',
    license='MIT',
    packages=['pygoogalytics'],
    install_requires=[
        'pandas',
        'google-api-python-client>=2.70.0',
        'google-analytics-data==0.16.1',
        'google-ads==20.0.0',
        'google-api-core==2.11.0'
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
