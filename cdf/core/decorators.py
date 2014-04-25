def feature_enabled(flag):
    """
    Temporary decorator to check if a task must be launched depending on features_enabled
    """
    def wrapper(func):
        def wrapped(*args, **kwargs):
            if 'features_flags' in kwargs:
                flags = kwargs.pop('features_flags')
                if flag in flags:
                    return func(*args, **kwargs)
                return None
            return func(*args, **kwargs)
        return wrapped
    return wrapper
