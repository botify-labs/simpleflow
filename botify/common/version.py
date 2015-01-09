class Version(object):
    MAJOR = 'major'
    MINOR = 'minor'
    MICRO = 'micro'

    def __init__(self, major, minor, micro):
        self.major = major
        self.minor = minor
        self.micro = micro

    def increment_major(self):
        self.major += 1
        self.minor = 0
        self.micro = 0
        return self

    def increment_minor(self):
        self.minor += 1
        self.micro = 0
        return self

    def increment_micro(self):
        self.micro += 1
        return self

    def bump(self, level=MICRO):
        getattr(self, 'increment_{}'.format(level))()
        return self

    @classmethod
    def fromstring(cls, string):
        return cls(*(int(i) for i in string.split('.')))

    def __str__(self):
        return '{major}.{minor}.{micro}'.format(
            major=self.major,
            minor=self.minor,
            micro=self.micro,
        )
