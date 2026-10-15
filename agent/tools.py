"""A handful of simple tools the example agent can call."""
import time


def get_time() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def word_count(text: str) -> int:
    return len(text.split())


def add(a: float, b: float) -> float:
    return a + b


TOOLS = {
    "get_time": get_time,
    "word_count": word_count,
    "add": add,
}
