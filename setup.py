from setuptools import setup, find_packages
from typing import List

PROJECT_NAME = "Machine Learning project"
VERSION = "0.0.1"
AUTHOR = "Praful Bhojane"
DESCRIPTION = "End to End ML project on Rental Bike Count Prediction dataset"

REQUIREMENTS_FILE_NAME = "requirements.txt"

HYPHEN_E_DOT = "-e."


def get_requirements_list() -> List[str]:
    """Description: This function create a list of all requirements that we need 
       for this project"""
    with open(REQUIREMENTS_FILE_NAME) as requirement_file:
        requirement_list = requirement_file.readlines()
        requirement_list = [requirement_name.replace("/n", "") for requirement_name in requirement_list]
        if HYPHEN_E_DOT in requirement_list:
            requirement_list.remove(HYPHEN_E_DOT)
        
        return requirement_list
setup(
    name = PROJECT_NAME,
    version = VERSION,
    author = AUTHOR,
    description= DESCRIPTION,
    packages = find_packages(),
    install_requires = get_requirements_list()
)   