# CacheIntervals: Memoization with interval parameters
#
# Copyright (C) Cyril Godart
#
# This file is part of CacheIntervals.
#
# @author = 'Cyril Godart'
# @email = 'cyril.godart@gmail.com'


import pytest
import unittest

class Test(unittest.TestCase):
    def setUp(self):
        self.a = 1

    def test_me(self):
        b = 1
        assert b == self.a
