from setuptools import setup, find_namespace_packages
from setuptools_rust import Binding, RustExtension, Strip


setup(
    name='fluvio',
    version="0.0.1",
    author = "Fluvio Contributors",
    author_email = "team@fluvio.io",
    packages=find_namespace_packages(include=['fluvio.*']),
    zip_safe=False,
    rust_extensions=[RustExtension("fluvio.fluvio_rust", path="Cargo.toml", binding=Binding.RustCPython, debug=False)],
)
