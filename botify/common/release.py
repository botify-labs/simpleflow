import subprocess
from distutils .core import Command

from botify.common.package import Package
from botify.common.git import Git
from botify.common.pypi import Pypi
from botify.common import github


class Release(Command):
    """

    Please specify the following section in the ``setup.cfg`` file at the
    top-level of the project:

        package=PACKAGE_NAME
        pypi=REPOSITORY_NAME_IN_PYPIRC
        url=PROJECT_URL

    """
    description = 'release the project'
    user_options = [
        ('package=', None, "package's name"),
        ('url=', None, 'URL of the project'),
        ('dry-run', None, 'dry run'),
        ('version=', 'v', 'version to release'),
        ('major', None, 'release major version'),
        ('minor', None, 'release minor version'),
        ('micro', None, 'release micro version (default)'),
        ('pypi=',
         None,
         'name (in ~/.pypirc) of the pypi repository to upload to'),
    ]

    def initialize_options(self):
        self.package = None
        self.url = None

        self.dry_run = False
        self.version = None
        self.major = False
        self.minor = False
        self.micro = True
        self.pypi = None

    def finalize_options(self):
        pass

    def run(self):
        pkg = Package(self.package)
        current = str(pkg.version)
        repo = Git()

        if self.major:
            pkg.version.increment_major()
        elif self.minor:
            pkg.version.increment_minor()
        elif self.micro:
            pkg.version.increment_micro()
        else:
            pkg.version.bump()
        print 'releasing {}=={}'.format(pkg.name, pkg.version)

        if repo.find_tag(str(pkg.version)):
            raise ValueError(
                'version {} already exists'.format(str(pkg.version))
            )

        if not self.dry_run:
            pkg.version.save()

        commit_message = 'Bump version to {}'.format(pkg.version)

        if not repo.find_tag(current):
            current = None

        changelog = repo.changelog(current)
        tag_message = (
            '{}\n\n'
            'ChangeLog:\n'
            '{}'.format(pkg.version, changelog)
        )

        commands = [
            ['git', 'add', pkg.version.path],
            ['git', 'commit', '-m', commit_message],
            ['git', 'tag', '-a', str(pkg.version), '-m', tag_message],
            ['git', 'push', 'origin', 'devel'],
            ['git', 'push', 'origin', str(pkg.version)]
        ]
        if not self.dry_run:
            for command in commands:
                subprocess.check_output(command)
        else:
            for command in commands:
                print " ".join(command)

        Pypi(self.pypi).upload(dry_run=self.dry_run)
        github.release(self.url, str(pkg.version), changelog, self.dry_run)
