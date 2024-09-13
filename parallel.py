import dataclasses
import logging
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
log_format = logging.Formatter("%(asctime)s [%(levelname)8s] %(message)s")
# Print log to stdout
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(log_format)
logger.addHandler(stdout_handler)
# Print log to file
file_handler = logging.FileHandler("hoge.log", "a+")
file_handler.setFormatter(log_format)
logger.addHandler(file_handler)

# Counter for progress display
SUCCESS_COUNTER_LOCK = threading.Lock()
SUCCESS_COUNTER = 0

ERROR_COUNTER_LOCK = threading.Lock()
ERROR_COUNTER = 0


@dataclasses.dataclass
class WorkerInput:
    """Worker input"""

    # TODO : Change according to the actual task
    id: int


@dataclasses.dataclass
class WorkerOutput:
    """Worker output"""

    # TODO : Change according to the actual task
    is_success: bool


class Worker:
    @staticmethod
    def run(input: WorkerInput) -> WorkerOutput:
        try:
            # TODO : Change according to the actual task (e.g. API call)
            time.sleep(10)
            # Fail occasionally
            if random.random() < 0.5:
                raise Exception("Random Error")
            logger.info(f"success. {input=}")
            # TODO: Task-specific processing

            with SUCCESS_COUNTER_LOCK:
                global SUCCESS_COUNTER
                SUCCESS_COUNTER += 1
            return WorkerOutput(is_success=True)

        except Exception as e:
            logger.warning(f"error. {input=} err={e}")
            with ERROR_COUNTER_LOCK:
                global ERROR_COUNTER
                ERROR_COUNTER += 1
            return WorkerOutput(is_success=False)


if __name__ == "__main__":
    worker_num = 3
    # TODO: Change according to the actual task
    input_list = [WorkerInput(id=i) for i in range(10)]
    input_num = len(input_list)

    with ThreadPoolExecutor(
        max_workers=worker_num, thread_name_prefix="thread"
    ) as executor:
        futures = []
        for input in input_list:
            # Start worker
            futures.append(executor.submit(Worker.run, input))

        # Loop until all workers are finished
        while True:
            # Display progress
            logger.info(
                f"success={SUCCESS_COUNTER} error={ERROR_COUNTER} total={input_num}"
            )
            # If all workers are finished, exit the loop
            if SUCCESS_COUNTER + ERROR_COUNTER == input_num:
                break
            time.sleep(10)

        # Get the result of the worker
        output_list: List[WorkerOutput] = [future.result() for future in futures]

        logger.info(f"{output_list=}")
