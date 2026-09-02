from __future__ import annotations

import unittest

import commerce_pipeline


class PackageTests(unittest.TestCase):
    def test_package_version(self) -> None:
        self.assertEqual(commerce_pipeline.__version__, "0.1.0")


if __name__ == "__main__":
    unittest.main()