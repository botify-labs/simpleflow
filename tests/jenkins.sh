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
pip install Cython==0.19.1
pip install numpy
pip install numexpr==2.1
pip install elasticsearch==0.4.1
pip install nose
pip install BQL

pip install python-google-analytics

python setup.py install
#ignore integration tests
nosetests --with-xunit -e=*integration*
[ $? -ne 0 ] && STATUS=1

deactivate
exit $STATUS
