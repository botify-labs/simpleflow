import os.path

from botify.common.version import Version


class PackageVersion(object):
    def __init__(self, pkg, location='.'):
        self._pkg = pkg
        self._version = Version.fromstring(
            __import__(pkg.name, fromlist=['*']).__version__,
        )

    def __set__(self, pkg, version):
        self._version = version

    def increment_major(self):
        return self._version.increment_major()

    def increment_minor(self):
        return self._version.increment_minor()

    def increment_micro(self):
        return self._version.increment_micro()

    def bump(self, level=Version.MICRO):
        return self._version.bump(level)

    @property
    def path(self):
        return os.path.join(self._pkg._location, self._pkg.name, '_version.py')

    def save(self):
        with open(self.path, 'w') as f:
            f.write("VERSION = '{}'\n".format(self._version))

    def __str__(self):
        return str(self._version)


class Package(object):
    def __init__(self, name, location='.'):
        self.name = name
        self._location = location
        self.version = PackageVersion(self)
