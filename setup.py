from setuptools import setup
from setuptools_rust import Binding, RustExtension

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="fluvio",
    version="0.21.0",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Fluvio Contributors",
    description="Python client library for Fluvio",
    python_requires=">=3.9",
    url="https://www.fluvio.io/",
    keywords=["fluvio", "streaming", "stream"],
    author_email="team@fluvio.io",
    project_urls={  # Optional
        "Bug Reports": "https://github.com/infinyon/fluvio-client-python/issues",
        "Source": "https://github.com/infinyon/fluvio-client-python",
    },
    # For a list of valid classifiers, see https://pypi.org/classifiers/
    classifiers=[  # Optional
        # Indicate who your project is intended for
        "Intended Audience :: Developers",
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate you support Python 3. These classifiers are *not*
        # checked by 'pip install'. See instead 'python_requires' below.
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3 :: Only",
    ],
    rust_extensions=[
        RustExtension(
            "fluvio._fluvio_python",
            path="Cargo.toml",
            binding=Binding.PyO3,
            quiet=True, 
        )
    ],
    packages=["fluvio"],
    install_requires=requirements,
    # rust extensions are not zip safe, just like C-extensions.
    zip_safe=False,
)
