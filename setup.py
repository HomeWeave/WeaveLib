try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='weavelib',
    version='0.8',
    author='Srivatsan Iyer',
    author_email='supersaiyanmode.rox@gmail.com',
    packages=['weavelib'],
    license='MIT',
    description='Library to interact with Weave Server',
    long_description=open('README.md').read(),
    install_requires=[
        'jsonschema'
    ],
    tests_requires=[
        'pytest'
    ]
)
