try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

#Parse requirements.txt
with open('requirements.txt') as f:
    required = f.read().splitlines()

config = {
    'description': 'Always know exactly what to expect from your data.',
    'author': 'Abe Gong',
    'url': 'https://github.com/abegong/great_expectations',
    'download_url': '...',
    'author_email': 'abegong@gmail.com',
    'version': '0.2.1',
    'install_requires': required,
    'packages': [
        'great_expectations',
        'great_expectations.dataset',
    ],
    'scripts': [
        'bin/great_expectations',
    ],
    'name': 'great_expectations'
}

setup(**config)
