def should_force_step(step_name, force_steps):
    """
    Check if step_name is in force_steps
    We support multi-level flags, ex for step_name = "a.b.c",
    we allow : "a", "a.b", "a.b.c"
    If one of force_steps is a wildcard (*), it will also force the step
    """
    for step in force_steps:
        if step == "*" or step == step_name or step_name.startswith(step + "."):
            return True
    return False


def should_skip_step(step_name, skipped_steps):
    return should_force_step(step_name, skipped_steps)


def step_will_run(step_name, force_steps, skipped_steps, steps_done, force=False):
    """
    Return True if step will run by checking :
    1/ force is True
    2/ step_name is in force_steps configuration
    3/ step_name is not yet computed
    4/ step_name is not skipped
    """
    # Forced steps has priority on skipped steps
    if force or should_force_step(step_name, force_steps):
        return True
    elif should_skip_step(step_name, skipped_steps):
        return False
    else:
        return step_name not in steps_done


def step_is_forced(step_name, force_steps, force):
    return (
        force or
        should_force_step(step_name, force_steps))


def _get_step_reasons(step_name, step_reasons):
    reasons = []
    for step, sreasons in step_reasons.items():
        if step == "*" or step == step_name or step_name.startswith(step + "."):
            reasons += sreasons
    return reasons


def get_step_force_reasons(step_name, step_force_reasons):
    return _get_step_reasons(step_name, step_force_reasons)


def step_is_skipped_by_force(step_name, skipped_steps):
    return should_skip_step(step_name, skipped_steps)


def get_step_skip_reasons(step_name, step_skip_reasons):
    return _get_step_reasons(step_name, step_skip_reasons)
