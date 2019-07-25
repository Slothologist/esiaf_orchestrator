from distutils.core import setup
from catkin_pkg.python_setup import generate_distutils_setup

# fetch values from package.xml
setup_args = generate_distutils_setup(
    name='esiaf_orchestrator',
    version='0.0.1',
    description='An orchestrator for the esiaf_ros framework',
    url='---none---',
    author='rfeldhans',
    author_email='rfeldh@gmail.com',
    license='---none---',
    install_requires=[
        'datetime',
        'sqlite3'
    ],
    packages=['esiaf_orchestrator', 'esiaf_orchestrator.fusion']

)

setup(**setup_args)