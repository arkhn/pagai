import os
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


requirements = read("requirements.txt").split()

setuptools.setup(
    name="pagai",
    version="0.1.0",
    author="Arkhn",
    author_email="contact@arkhn.org",
    description="Pagai is a SQL database inspection tool implemented in Python.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/arkhn/pagai/",
    packages=setuptools.find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
