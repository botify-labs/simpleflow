def pad_list(input_list, goal_length, fill_value):
    """Pad elements at the end of a list so that it reach a goal length.
    :param input_list: the input list
    :type input_list: list
    :param goal_length: the size of the returned list
    :type goal_lenght: int
    :param fill_value: the value of the padding elements
    :type fill_value: unknown
    :returns: list"""
    length = len(input_list)
    if length >= goal_length:
        return input_list
    else:
        return input_list + [fill_value] * (goal_length - length)


