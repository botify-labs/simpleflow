import subprocess

SDIST = 'sdist'
BDIST_WHEEL = 'bdist_wheel'

DIST_TYPES = (
    SDIST,
    BDIST_WHEEL,
)


class Pypi(object):
    def __init__(self, name):
        self.name = name

    def upload(self, dist_type=BDIST_WHEEL, dry_run=False):
        """Create the python package and upload it to pypi.

        :param dry_run: if True, nothing is actually done.
                        The function just prints what it would do.

        :type dry_run: bool.

        """
        if dist_type not in DIST_TYPES:
            raise ValueError('invalid dist type "{}": must be in {}'.format(
                dist_type,
                ', '.join(DIST_TYPES)))

        command = 'python setup.py {dist_type} upload -r {repo}'.format(
            dist_type=dist_type,
            repo=self.name,
        )

        if not dry_run:
            print subprocess.check_output(command, shell=True)
        else:
            print command
