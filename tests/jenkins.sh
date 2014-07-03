#!/usr/bin/bash

#a jenkins script to be launched by the botify-cdf jenkins job
STATUS=0

#get script location
#from http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $SCRIPT_DIR/.. #back to root directory

VIRTUALENV_DIR="/tmp/venv"
virtualenv $VIRTUALENV_DIR
[ $? -ne 0 ] && exit 1
source $VIRTUALENV_DIR/bin/activate

#install dependencies
pip install pip-accel

pip-accel install Cython==0.19.1
pip-accel install numpy
pip-accel install numexpr==2.1
pip-accel install elasticsearch==0.4.1
pip-accel install nose
pip-accel install BQL

pip-accel install python-google-analytics

pip-accel install coverage
pip-accel install mock
pip-accel install httpretty==0.7.0
pip-accel install moto

python setup.py install
#ignore integration tests
nosetests --with-xunit --with-coverage --cover-package=cdf --cover-xml -e=*integration*
[ $? -ne 0 ] && STATUS=1
sloccount --duplicates --wide --details cdf > sloccount.out

deactivate
exit $STATUS
