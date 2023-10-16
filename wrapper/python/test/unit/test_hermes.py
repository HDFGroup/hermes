from unittest import TestCase
from py_hermes import Hermes, TRANSPARENT_HERMES
import pathlib
import os

class TestHermes(TestCase):
    def test_metadata_query(self):
        TRANSPARENT_HERMES()
        hermes = Hermes()
        mdm = hermes.CollectMetadataSnapshot()
        print(mdm.blob_info)
        print("Done")
