import os
from setuptools import setup, find_packages

def get_version():
    init_path = os.path.join(os.path.dirname(__file__), "..", "leviathan_common", "leviathan_common", "__init__.py")
    with open(init_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("__version__"):
                return line.split("=")[1].strip().strip('"').strip("'")
    return "0.0.0"

setup(
    name="leviathan_streamers",
    version=get_version(),
    packages=find_packages(),
    python_requires=">=3.8",
)
