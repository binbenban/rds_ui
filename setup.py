from setuptools import setup, find_packages
from os import path


name = "odapui"
here = path.abspath(path.dirname(__file__))

# get the dependencies and installs
with open("requirements.txt", "r", encoding="utf-8") as f:
    requires = [x.strip() for x in f if x.strip()]

test_requirements = ["pytest"]
setup(
    name=name,
    version="0.1",
    packages=find_packages("src", include=[name]),
    package_dir={"": "src"},
    include_package_data=True,
    tests_require=test_requirements,
    install_requires=requires,
    extras_require={"testing": test_requirements},
)
