#!/usr/bin/env python

import logging
import functools

# logging
logging.basicConfig(format="%(levelname)s\t%(asctime)s\t%(message)s",filename="ogg.log")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Decorator
def log(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.debug('\ncall %s():' % func.__name__)
        return func(*args, **kwargs)
    return wrapper
