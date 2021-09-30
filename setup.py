from setuptools import setup, find_namespace_packages
from setuptools_rust import Binding, RustExtension, Strip

setup(
    name='fluvio',
    version="0.9.6-beta-3",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author = "Fluvio Contributors",
    description='Python client library for Fluvio',
    python_requires='>=3.6',
    url='https://www.fluvio.io/',
    keywords=['fluvio', 'streaming', 'stream'],
    license='APACHE',
    author_email = "team@fluvio.io",
    setup_requires=['wheel'],
    project_urls={  # Optional
        'Bug Reports': 'https://github.com/infinyon/fluvio-client-python/issues',
        'Source': 'https://github.com/infinyon/fluvio-client-python',
    },
    # For a list of valid classifiers, see https://pypi.org/classifiers/
    classifiers=[  # Optional
        # Indicate who your project is intended for
        'Intended Audience :: Developers',

        # Pick your license as you wish
        'License :: OSI Approved :: Apache Software License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate you support Python 3. These classifiers are *not*
        # checked by 'pip install'. See instead 'python_requires' below.
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3 :: Only',
    ],

    rust_extensions=[RustExtension("fluvio._fluvio_python", path="Cargo.toml", binding=Binding.RustCPython, debug=False)],
    packages=["fluvio"],
    # rust extensions are not zip safe, just like C-extensions.
    zip_safe=False,
)
