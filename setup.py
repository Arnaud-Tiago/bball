from setuptools import find_packages
from setuptools import setup

with open("requirements.txt") as f:
    content = f.readlines()
requirements = [x.strip() for x in content if "git+" not in x]

setup(name='basket-ball',
      version="0.0.1",
      description="Basket-Ball project",
      author="A.Odet",
      author_email="arnaud.odet.pro@gmail.com",
      url="https://github.com/Arnaud-Tiago/basket-ball",
      install_requires=requirements,
      packages=find_packages(),
      test_suite="tests",
      # include_package_data: to install data from MANIFEST.in
      include_package_data=True,
      zip_safe=False)
