import logging
import sys

logger = logging.getLogger("streamline")  # use a consistent name

if not logger.hasHandlers():  # avoid duplicate handlers in some environments
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    handler.setFormatter(formatter)

    logger.addHandler(handler)

logger.propagate = False
