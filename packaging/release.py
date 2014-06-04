"""This script releases versions of cdf.
It :
- bumps the version,
- tags it
- push the bump commit to github
- push the tag to github
- upload the pip package to pypo

Concerning the version numbers, the script reads the previous release version
from cdf/__init__.py. It increments its micro version and modify
cdf/__init__.py consequently.


Limitations:
This script only increases the micro version,
if you want to increase the major or minor version, you will have to do it
by yourself.

Releases can not be launched by jenkins as
it does not have sufficient rights
"""

import argparse
import subprocess
import os.path
import fileinput
import re

#get the path to the local cdf directory
regex = ".*/botify-cdf"
LOCAL_PACKAGE_PATH = re.search(regex, os.path.abspath(__file__)).group(0)

#modify sys.path so that the local cdf is loaded
import sys
sys.path.insert(0, LOCAL_PACKAGE_PATH)

import cdf


def get_last_release_version():
    """Returns the version number of the last release
    as a tuple (major, minor, micro).
    :returns: contents_duplicate
    """
    #get current VERSION
    release_version = cdf.__version__
    return [int(i) for i in release_version.split(".")]


def get_release_version():
    """Returns the version number of the next release version
    as a tuple (major, minor, micro).
    :returns: tuple
    """
    major, minor, micro = get_last_release_version()
    #increment micro version and reset micro version
    result = [major, minor, micro + 1]
    result = [int(i) for i in result]
    result = tuple(result)
    return result


def get_init_filepath():
    """Return the path to cdf.__init__.py file
    :returns: str
    """
    filepath = os.path.join(os.path.dirname(cdf.__file__),
                            "__init__.py")
    return filepath


def set_version(version):
    """Change the version of the package.
    This function modifies __init__.py
    :param version: the new version to set as a tuple of integers
                    (major, minor, micro)
    :param version: tuple
    """
    #find file location
    filename = get_init_filepath()
    regex = re.compile("version\s*=\s*\(\d+, \d+, \d+\)")
    #inplace replacement
    #cf http://stackoverflow.com/questions/39086/search-and-replace-a-line-in-a-file-in-python
    for line in fileinput.input(filename, inplace=True):
        if regex.match(line):
            sys.stdout.write("version = {}\n".format(str(version)))
        else:
            sys.stdout.write(line)


def upload_package(dry_run):
    """Create the python package
    and upload it to pypi
    :param dry_run: if True, nothing is actually done.
                    the function just prints what it would do
    :type dry_run: bool"""
    command = ["python", "setup.py", "sdist", "upload", "-r", "botify"]
    if not dry_run:
        subprocess.check_output(command)
    else:
        print " ".join(command)


def release_official_version(dry_run):
    """Release an official version of cdf
    :param dry_run: if True, nothing is actually done.
                    the function just prints what it would do
    :type dry_run: bool"""
    #bump version
    version = get_release_version()
    tag = ".".join([str(i) for i in version])
    print "Creating cdf {}".format(tag)
    #in case of dry run, we do not want to modify the files
    if not dry_run:
        set_version(version)
    init_filepath = get_init_filepath()
    commit_message = "bump version to {}".format(tag)

    commands = [
        #commit version bump
        ["git", "add", init_filepath],
        ["git", "commit", "-m", commit_message],
        #tag current commit
        ["git", "tag", "-a", tag, "-m", tag],
        #push commits
        ["git", "push", "origin", "devel"],
        #upload package
        ["git", "push", "origin", tag]
    ]
    if not dry_run:
        for command in commands:
            subprocess.check_output(command)
    else:
        for command in commands:
            print " ".join(command)

    #upload package
    upload_package(dry_run)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Release version of cdf.'
    )

    parser.add_argument('-f',
                        dest="force",
                        default=False,
                        action="store_true",
                        help='Force release')

    parser.add_argument('-n',
                        dest="dry_run",
                        default=False,
                        action="store_true",
                        help='Dry run')

    args = parser.parse_args()

    if not args.force and not args.dry_run:
        raise ValueError("You must choose option '-f' or '-n'")

    if args.force and args.dry_run:
        raise ValueError("You cannot choose both options '-f' and '-n'")

    release_official_version(args.dry_run)
