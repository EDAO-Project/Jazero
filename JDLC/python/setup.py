from distutils.core import setup

setup(
    name = 'jdlc',
    packages = ['jdlc'],
    version = '0.1',
    license='MIT',
    description = 'JDLC Python package for interacting with the Jazero semantic data lake.',
    author = 'Martin Pekár',
    author_email = 'your.email@domain.com',
    url = 'https://github.com/EDAO-Project/Jazero',
    download_url = 'https://github.com/EDAO-Project/Jazero/archive/refs/tags/jazero-v0.1.0.tar.gz',
    keywords = ['semantic data lake', 'connector', 'API'],
    install_requires=[
        'requests',
        'shutils',
        "simplejson",
        "argparse"
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)