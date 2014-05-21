def compute_average_value(sum_values, total_sessions):
    """Compute the average of a metric.
    If the number of sessions is null, returns 0.
    The returned result is rounded to two decimals
    :param sum_values: the sum of the values of the considered metric
    :value sum_values: int
    :param total_sessions: the total number of sessions
    :param total_sessions: int
    :returns: float
    """
    if total_sessions != 0:
        result = float(sum_values)/float(total_sessions)
    else:
        result = 0.0
    result = round(result, 2)
    return result


def compute_percentage(concerned_sessions, total_sessions):
    """Compute the percentage of sessions concerned by a property.
    If the number of sessions is null, returns 0.
    The returned result is rounded to two decimals
    :param concerned_session: the number of concerned sessions
    :value sum_values: int
    :param total_sessions: the toal number of sessions
    :param total_sessions: int
    :returns: float
    """
    if total_sessions != 0:
        result = 100 * float(concerned_sessions)/float(total_sessions)
    else:
        result = 0.0
    result = round(result, 2)
    return result
