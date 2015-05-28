from enum import Enum


NAME = "Link Graph"
DESCRIPTION = "Retrieve outlinks, inlinks and its status (follow, no-follow), canonicals and redirections"
ORDER = 100

NB_TOP_ANCHORS = 5

GROUPS = Enum(
    'Groups',
    [
        ("outlinks_internal", "Outlinks to Internal URLs"),
        ("outlinks_internal_nofollow", "Outlinks in Nofollow Mode to Internal URLs"),
        ("outlinks_external", "Outlinks to External URLs"),
        ("outlinks_external_nofollow", "Outlinks in NoFollow Mode to External URLs"),
        ("inlinks", "Inlinks in Follow Mode"),
        ("inlinks_nofollow", "Inlinks in Nofollow Mode"),
        ("redirects", "Redirects"),
        ("canonical", "Canonical Tags"),
        ("page_rank", "Internal Page Rank")
    ]
)
