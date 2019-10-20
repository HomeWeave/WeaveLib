from setuptools import setup, find_packages


setup(
    name='weavelib',
    version='0.8',
    author='Srivatsan Iyer',
    author_email='supersaiyanmode.rox@gmail.com',
    packages=find_packages(),
    license='MIT',
    description='Library to interact with Weave Server',
    long_description=open('README.md').read(),
    install_requires=[
        'jsonschema',
        'netifaces',
        'psutil',
    ]
)
