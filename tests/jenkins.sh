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

#pip management
PIP="pip-accel"
if [ $PIP = "pip-accel" ]; then
    pip install pip-accel
fi

#install dependencies
for REQUIREMENT in $(cat pip_requirements.txt)
do
    $PIP install --timeout 180 $REQUIREMENT
done

$PIP install python-google-analytics

$PIP install nose
$PIP install coverage
$PIP install mock
$PIP install httpretty==0.7.0
$PIP install moto

python setup.py install
#ignore integration tests
nosetests --with-xunit --with-coverage --cover-package=cdf --cover-xml -e=*integration*
[ $? -ne 0 ] && STATUS=1
sloccount --duplicates --wide --details cdf > sloccount.out

deactivate
exit $STATUS
