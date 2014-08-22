#!/usr/bin/bash

#a jenkins script to be launched by the botify-cdf jenkins job
STATUS=0

#get script location
#from http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $SCRIPT_DIR/.. #back to root directory

#configure pip if necessary
export PIP_CONFIG_FILE=$PWD/pip.conf
cat > $PIP_CONFIG_FILE << EOF
[global]
index-url = http://ff879dbbee9047288c13e31e9f8d45d3:dc076cf5483e4501a548225cbbf5b9b2@pypi.botify.com/simple
EOF

VIRTUALENV_DIR="/tmp/venv"
virtualenv $VIRTUALENV_DIR --system-site-packages
[ $? -ne 0 ] && exit 1
source $VIRTUALENV_DIR/bin/activate

#pip management
PIP="pip"
if [ $PIP = "pip-accel" ]; then
    pip install pip-accel
fi

#install runtime and test dependencies
for REQUIREMENT in $(cat packaging/python.deps packaging/python_test.deps)
do
    $PIP install --timeout 180 $REQUIREMENT
done

$PIP install python-google-analytics

pip install -e .
#ignore integration tests
nosetests --with-xunit --with-coverage --cover-package=cdf --cover-xml -e=*integration*
[ $? -ne 0 ] && STATUS=1
sloccount --duplicates --wide --details cdf > sloccount.out

deactivate
exit $STATUS
