#!/usr/bin/env python
import os
import re
import subprocess
import sys


CHANGELOG_FILE = "HISTORY.rst"


def current_version():
    # This command looks weird but it hides dozens of attempts to get the
    # most correct thing, so don't touch it unless you're sure
    cmd = "git for-each-ref --format='%(*committerdate:raw)%(committerdate:raw) %(refname)' refs/tags"
    cmd += " | sort -n | tail -n 1 | awk '{print $3}'"
    return subprocess.check_output(cmd, shell=True).replace("refs/tags/", "").strip()


def changelog_lines(from_tag=None):
    if not from_tag:
        from_tag = current_version()

    cmd = ["git", "log", "--pretty=format:- %b (%s)",
           "--merges", "{}..".format(from_tag)]
    out = subprocess.check_output(cmd)
    for line in out.splitlines():
        line = re.sub(r"\(Merge pull request (#\d+)[^)]+\)", r"(\1)", line)
        yield line


def proposed_changelog(from_tag, new_tag):
    return "\n{version}\n{underline}\n\n{content}\n".format(
        version=new_tag,
        underline="~" * len(new_tag),
        content="\n".join(list(changelog_lines(from_tag)))
    )


def write_changelog(content, new_tag):
    with open(CHANGELOG_FILE, "r") as f:
        current_changelog = f.readlines()

    # safeguard for not documenting the same tag twice
    if new_tag+"\n" in current_changelog:
        raise ValueError("The tag {} is already present in {}".format(new_tag, CHANGELOG_FILE))

    # detect where the first sub-title begins, it will be the first version
    # section ; we will introduce our new changelog here
    first_version_line_number = [
        idx for idx, line in enumerate(current_changelog)
        if line.startswith("~~~")
    ][0] - 2

    tmp_file = CHANGELOG_FILE + ".new"
    with open(tmp_file, "w") as f:
        for idx, line in enumerate(current_changelog):
            if idx == first_version_line_number:
                f.write(content)
            f.write(line)

    os.rename(tmp_file, CHANGELOG_FILE)
    print "=> {} updated successfully".format(CHANGELOG_FILE)


if __name__ == "__main__":
    new_tag = sys.argv[1]
    from_tag = sys.argv[2] if len(sys.argv) > 2 else None

    print "=> proposed changelog content:"
    proposed = proposed_changelog(from_tag, new_tag)
    print proposed.replace("\n", "\n   ")

    print "=> is this ok? [Y/n]",
    answer = raw_input()
    if answer.startswith("n"):
        print "aborting."
    else:
        write_changelog(proposed, new_tag)
