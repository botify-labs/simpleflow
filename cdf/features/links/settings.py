from enum import Enum


NAME = "Link Graph"
DESCRIPTION = "Retrieve outlinks, inlinks and its status (follow, no-follow), canonicals and redirections"
ORDER = 100


GROUPS = Enum(
    'Groups',
    [
        ("outlinks_internal", "Outlinks Internal in Follow"),
        ("outlinks_internal_nofollow", "Outlinks Internal in No-Follow"),
        ("outlinks_external", "Outlinks External"),
        ("outlinks_external_nofollow", "Outlinks External in No-Follow"),
        ("inlinks", "Inlinks in Follow"),
        ("inlinks_nofollow", "Inlinks in No-Follow"),
        ("redirects", "Redirects"),
        ("canonical", "Canonicals"),
    ]
)


