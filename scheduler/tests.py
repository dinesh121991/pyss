#!/usr/bin/env python2.4
import unittest

class tests(unittest.TestCase):
    pass

if __name__ == "__main__":
    try:
        from testoob import main
    except ImportError:
        from unittest import main
    main()
