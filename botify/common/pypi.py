import subprocess


class Pypi(object):
    def __init__(self, name):
        self.name = name

    def upload(self, dry_run=False):
        """Create the python package and upload it to pypi.

        :param dry_run: if True, nothing is actually done.
                        The function just prints what it would do.

        :type dry_run: bool.

        """
        command = 'python setup.py bdist_wheel upload -r {repo}'.format(
            repo=self.name)

        if not dry_run:
            print subprocess.check_output(command, shell=True)
        else:
            print command
