from setuptools import setup, find_packages

setup(
    name="rapid",
    version="0.0.7",
    description="A python sdk for the rAPId API",
    url="https://github.com/no10ds/rapid-sdk",
    author="Lewis Card",
    author_email="lcard@no10.gov.uk",
    license="MIT",
    packages=find_packages(include=["rapid", "rapid.*"], exclude=["tests"]),
    install_requires=["pandas", "requests"],
    include_package_data=True,
)
