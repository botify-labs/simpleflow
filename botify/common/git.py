import subprocess


class Git(object):
    WIDTH = 80
    INDENT = 4
    LOG_FORMAT = "- %h %s [%an]%n%w({width},{indent},{indent})%b".format(
        width=WIDTH,
        indent=INDENT,
    )

    def changelog(self, commit=None):
        """Generate a changelog from a given tag to HEAD

        :param tag: the reference tag
        :type tag: str

        :returns:
            :rtype: str

        """
        #each commit is formatted this way
        #-  Merge pull request #392 from sem-io/feature/foo [John Doe]
        #
        #  Very useful commit message because:
        #  - it lists what the commit does
        #
        #cf man git log for further explanations on the syntax
        #the trickiest part is the %w(...)
        #that configures the commit message indentation
        if commit is None:
            ref = 'HEAD'
        else:
            ref = '{}..HEAD'.format(commit)

        command = [
            "git", "log", ref,
            "--first-parent", '--pretty=format:{}'.format(self.LOG_FORMAT),
        ]
        return subprocess.check_output(command)

    def find_tag(self, tag):
        return (
            subprocess.check_output(['git', 'tag', '-l', tag]).strip() == tag
        )
