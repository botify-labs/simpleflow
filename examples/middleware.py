def my_pre_execution_middleware(context, **kwargs):
    print("AAAH PRE EXECUTION MIDDLEWARE", context)


def my_post_execution_middleware(context, result, **kwargs):
    print("AAAH POST EXECUTION MIDDLEWARE", "activity result:", result)
