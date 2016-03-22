class Response(object):
    """
    This class wraps SWF responses so they can be passed around and examined later.
    We used to do that with raw python structures, but it's too hard to change
    things afterwards when you return, for instance, a tuple. Adding a value to the
    returned ones breaks the method signature and potential methods using it.
    """
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.__setattr__(k, v)
