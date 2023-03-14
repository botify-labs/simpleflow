from simpleflow import Workflow, activity
from simpleflow.exceptions import ExecutionBlocked

from .worker import build_boat, find_crew, find_or_steal_money, find_parrot, steal_boat


# decorator for all activities
def pirate_activity(func):
    return activity.with_attributes(
        task_list="pirate",
        version="1.0",
    )(func)


# wrap activities
find_or_steal_money = pirate_activity(find_or_steal_money)
build_boat = pirate_activity(build_boat)
steal_boat = pirate_activity(steal_boat)
find_crew = pirate_activity(find_crew)
find_parrot = pirate_activity(find_parrot)


# decider logic
class PirateBusiness(Workflow):
    name = "pirate-business"
    task_list = "captain"
    version = "1.0"

    def run(
        self,
        money_needed=150,
    ):
        # get money
        money = 0
        while money < money_needed:
            money = self.submit(find_or_steal_money, initial=money, target=money_needed).result

        # build boat / crew
        a1 = self.submit(build_boat)
        a2 = self.submit(steal_boat)
        a3 = self.submit(find_crew)
        self.submit(find_parrot)

        # wait for everything to finish
        # NAIVE: futures.wait(a1, a2, a3)
        ok = a3.finished and (a1.finished or a2.finished)
        if not ok:
            raise ExecutionBlocked()

        # finished!
        print("Arr! Let's go test this boat!")
