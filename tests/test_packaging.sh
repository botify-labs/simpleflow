#!/usr/bin/bash
#A script to be used in a jenkins job.
#It builds a package from the sources.
#Then it tries to install it locally.

STATUS=0

#Test package creation
PACKAGE_DIR="dist"
rm -rf $PACKAGE_DIR
mkdir -p $PACKAGE_DIR
SDIST_ERROR_LOG="sdist.errors"
python setup.py sdist --dist-dir $PACKAGE_DIR sdist 2> $SDIST_ERROR_LOG 1>/dev/null
[ $? -ne 0 ] && STATUS=1
NB_ERRORS=$(cat $SDIST_ERROR_LOG |grep -E 'error'|wc -l)
if [ $NB_ERRORS -gt 0 ];
then
    cat $SDIST_ERROR_LOG
    STATUS=1
    exit $STATUS
fi
rm $SDIST_ERROR_LOG

#Test package installation
ARCHIVE=$(find $PACKAGE_DIR -type f) #there should be only one file in dist directory

VIRTUALENV_PATH="venv_packaging"
virtualenv $VIRTUALENV_PATH
source $VIRTUALENV_PATH/bin/activate

PIP_LOG="pip.log"
pip install -U --no-deps $ARCHIVE --log $PIP_LOG
[ $? -ne 0 ] && STATUS=1

NB_ERRORS=$(cat $PIP_LOG |grep -E 'error'|wc -l)
if [ $NB_ERRORS -gt 0 ];
then
    cat $PIP_LOG
    STATUS=1
fi
rm $PIP_LOG

deactivate
rm -rf $VIRTUALENV_PATH
exit $STATUS
