from collections import namedtuple


Reason = namedtuple('Reason', ['name', 'code'])
REASON_HTTP_CODE = Reason('http_code', 1)  # 0001
REASON_NOINDEX = Reason('noindex', 2)  # 0010
REASON_CONTENT_TYPE = Reason('content_type', 4)  # 0100
REASON_CANONICAL = Reason('canonical', 8)  # 1000

Reasons = [
    REASON_HTTP_CODE,
    REASON_CONTENT_TYPE,
    REASON_CANONICAL,
    REASON_NOINDEX
]


def encode_reason_mask(*reasons):
    """Encode reasons into a bit mask value"""
    mask = 0
    for reason in reasons:
        mask |= reason.code
    return mask


def decode_reason_mask(mask, all_reasons=Reasons):
    """Decode a bit mask value according to some pre-defined reasons"""
    reasons = []
    for reason in all_reasons:
        if (int(mask) & reason.code) == reason.code:
            reasons.append(reason)
    return reasons