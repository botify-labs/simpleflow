"""This script release versions of cdf
There are two kinds of release:
- dev releases: without any tags
- offficial releases: with tag and version bump

Official releases can not be launched by jenkins as
it does not have sufficient rights
"""

import argparse
import subprocess
import os.path
import fileinput
import re

import cdf


def get_last_release_version():
    #get current VERSION
    release_version = cdf.__version__
    return [int(i) for i in release_version]


def get_dev_number():
    """Return the current dev number.
    It is simply the number of commit since last tag.
    It has the advantage of monotony (always increase)
    However it does not garantee that the dev versions are consecutive.
    """
    #use --match option to get only tags of the form x.x.x
    label = subprocess.check_output(["git", "describe", "--match", "*.*.*"])
    #'git describe' usually  returns something like 0.1.29-95-gaf4ad71
    #with
    # - 0.1.29 the latest tag
    # - 95 the number of commits since latest tag
    # - gaf4ad71 the sha1 of the latest version
    #
    #but when it is run on a tag it simply return the tag
    version_chunks = label.split("-")
    if len(version_chunks) == 3:
        version, nb_commits, sha1 = version_chunks
        result = int(nb_commits)
    else:
        result = 0
    return result


def get_dev_suffix():
    """Return the suffix to apply to dev versions"""
    dev_number = get_dev_number()
    return "dev%s" % dev_number


def get_dev_version():
    """Return the current dev version number"""
    major, minor, micro = get_last_release_version()
    #increment minor version and reset micro version (implicit)
    result = [major, minor + 1]
    result = [str(i) for i in result]

    #append dev suffix
    result.append(get_dev_suffix())
    result = ".".join(result)
    return result


def get_release_version():
    major, minor, micro = get_last_release_version()
    #increment micro version and reset micro version
    result = [major, minor, micro + 1]
    result = [str(i) for i in result]

    result = ".".join(result)
    return result


def get_init_filepath():
    filepath = os.path.join(os.path.dirname(cdf.__file__),
                            "__init__.py")
    return filepath


def set_version(version):
    #find file location
    filename = get_init_filepath()
    regex = re.compile("VERSION\s*=\s*'\d+\.\d+\.\d+'")
    #inplace replacement
    #cf http://stackoverflow.com/questions/39086/search-and-replace-a-line-in-a-file-in-python
    for line in fileinput.input(filename, inplace=True):
        if regex.match(line):
            print "VERSION = '%s'" % version
        else:
            print line


def upload_package():
    """Create the python package
    and upload it to pypi"""
    subprocess.check_output(["python", "setup.py", "sdist", "upload", "-r", "botify"])


def release_official_version():
    """Release an official version of cdf"""
    #bump version
    version = get_release_version()
    print "Creating cdf %s" % version
    set_version(version)
    #commit version bump
    init_filepath = get_init_filepath()
    commit_message = "bump version to %s" % version
    subprocess.check_output(["git", "add", init_filepath])
    subprocess.check_output(["git", "commit", "-m", commit_message])
    #tag current commit
    subprocess.check_output(["git", "tag", "-a", version, "-m", version])

    #push commits
    subprocess.check_output(["git", "push", "origin"])
    #push tag
    subprocess.check_output(["git", "push", "origin", version])

    #upload package
    upload_package()


def release_dev_version():
    """Create a dev version of cdf and upload it to pypi"""
    version = get_dev_version()
    print "Creating cdf %s" % version
    #update __init__.py with version
    set_version(version)

    #upload package
    upload_package()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Release version of cdf.'
    )

    parser.add_argument('--devel',
                        default=False,
                        action="store_true",
                        help='Create an devel release')

    parser.add_argument('--official',
                        default=False,
                        action="store_true",
                        help='Create an official release')

    args = parser.parse_args()

    if not args.devel and not args.official:
        raise ValueError("You must choose option '--devel' or '--official'")

    if args.devel and args.official:
        raise ValueError("You can choose both options '--devel' and '--official'")

    if args.official:
        release_official_version()
    else:
        release_dev_version()
