import time
import random


def sleep_random(min_s, max_s, unit=1):
    seconds = random.randint(min_s, max_s)
    time.sleep(seconds * unit)


def find_or_steal_money(initial, target):
    current = initial
    loop = 0
    while current < target:
        sleep_random(1, 3, 0.1)
        current += 1
        loop += 1
        if loop > 50:
            return current
    return current


def build_boat():
    sleep_random(4, 14)


def steal_boat():
    sleep_random(8, 19)


def find_crew():
    sleep_random(4, 10)


def find_parrot():
    if random.randint(1, 2) == 1:
        raise ValueError("Failed!")
    sleep_random(1, 25)
