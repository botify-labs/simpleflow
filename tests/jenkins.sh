#!/usr/bin/bash

#a jenkins script to be launched by the job
#botify-cdf
STATUS=0

#get script location
#from http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $SCRIPT_DIR/.. #back to root directory

virtualenv /tmp/venv
[ $? -ne 0 ] && exit 1
source /tmp/venv/bin/activate

#install dependencies

pip install Cython==0.19.1
pip install numpy
pip install numexpr==2.1
pip install nose
pip install BQL

python setup.py install
nosetests --with-xunit
[ $? -ne 0 ] && STATUS=1

deactivate
exit $STATUS
