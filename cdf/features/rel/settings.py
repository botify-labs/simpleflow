from enum import Enum


NAME = "rel"
DESCRIPTION = "Retrieve <rel> anchors from HTML Pages"
ORDER = 100

GROUPS = Enum('Groups',
              [
                ('hreflang_outgoing', 'Outgoing Href Lang'),
                ('hreflang_incoming', 'Incoming Href Lang')
              ])

