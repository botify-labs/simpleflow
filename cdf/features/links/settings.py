NAME = "Link Graph"
DESCRIPTION = "Retrieve outlinks, inlinks and its status (follow, no-follow), canonicals and redirections"
PRIORITY = 100

GROUPS = [
    {"id": "outlinks_internal", "name": "Outlinks Internal in Follow"},
    {"id": "outlinks_internal_nofollow", "name": "Outlinks Internal in No-Follow"},
    {"id": "outlinks_external", "name": "Outlinks External"},
    {"id": "inlinks", "name": "Inlinks in Follow"},
    {"id": "inlinks_nofollow", "name": "Inlinks in No-Follow"},
    {"id": "redirects", "name": "Redirects"},
    {"id": "canonical", "name": "Canonicals"},
]
