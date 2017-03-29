class Submittable(object):
    """
    Object directly submittable to an executor, without wrapping:
    E.g. an ActivityTask but not an Activity.
    """
    pass


class SubmittableContainer(object):
    """
    Objects where submission returns either Submittable or SubmittableContainer objects

    We cannot pass those objects directly to the executor
    """
    pass
