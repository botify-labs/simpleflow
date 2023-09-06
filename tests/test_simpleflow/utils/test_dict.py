from simpleflow.utils import remove_none


def test_remove_none():
    before = {
        "a_list": [1, 2, None, 3],
        "a_dict": {
            "a": 1,
            "b": None,
            "c": 3,
            "d": {
                "nested": None,
            },
            "e": {
                "nested": {
                    "a": 1,
                    "b": None,
                },
            },
        },
    }

    expected = {
        "a_list": [1, 2, 3],
        "a_dict": {
            "a": 1,
            "c": 3,
            "d": {},
            "e": {
                "nested": {
                    "a": 1,
                },
            },
        },
    }

    assert remove_none(before) == expected
