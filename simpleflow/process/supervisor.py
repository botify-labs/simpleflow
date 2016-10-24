import multiprocessing


class Supervisor(object):
    """
    The `Supervisor` class is responsible for managing one or many worker processes
    in parallel. Those processes can be "deciders" or "activity workers" in the
    SWF terminology.

    It's heavily inspired by the process Supervisor from honcho (which is a clone of
    the "foreman" process manager, in python): https://github.com/nickstenning/honcho
    It also has its roots in the former simpleflow process manager and some of Botify
    private code which wasn't really well tested, and was re-written in a TDD-y
    style.
    """
    def __init__(self):
        pass

    def boot(self):
        """
        Used to start the Supervisor process once it's configured. Has to be called
        explicitly on a Supervisor instance so it starts (no auto-start from __init__()).
        """
        p = multiprocessing.Process(target=self.target)
        p.start()

    def target(self):
        """
        Supervisor's main "target", as defined in the `multiprocessing` API. It's the
        code that the manager will execute once booted.
        """
        import time
        time.sleep(60)
