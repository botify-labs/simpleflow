from simpleflow import Workflow, futures, logger


class CANWorkflow(Workflow):
    name = "continue_as_new"
    version = "1.0"
    task_list = "example"

    def run(self, i: int = 0) -> None:
        logger.info(
            "Okay, campers, rise and shine, and don’t forget your booties ’cause it’s cooooold out there today. "
        )
        logger.info("Run context: %r", self.get_run_context())
        logger.info("Last event ID: %d", self.executor.history.last_event_id)
        if i >= 10:
            logger.info("Finishing")
        else:
            futures.wait(self.submit(self.continue_as_new(i=i + 1)))
