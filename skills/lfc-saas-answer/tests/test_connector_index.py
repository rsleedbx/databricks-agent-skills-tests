"""test_connector_index.py: connector_index.py's parsing on sample pages (no network). Run:
python3 -m unittest -q skills/lfc-saas-answer/tests/test_connector_index.py"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
import connector_index as ci

PAGE = ('<html><head><title>Ingest data from Example | Databricks on AWS</title></head><body>'
        '<h1>Ingest data from Example</h1><p>This feature is in Beta</p><h2>Overview</h2>'
        '<p>This feature is in Public Preview for formulas</p>'
        '<a href="https://docs.databricks.com/aws/en/notebooks/source/saas-cdc/example-pipeline.html">n</a></html>')


class ConnectorIndexTest(unittest.TestCase):
    def test_status_is_the_badge_above_the_first_section(self):
        self.assertEqual(ci.status_of(PAGE), "Beta")
        self.assertEqual(ci.status_of("<h1>Plain</h1><h2>x</h2>"), "")

    def test_name_drops_the_prefix_and_the_site_suffix(self):
        self.assertEqual(ci.name_of(PAGE, "example"), "Example")

    def test_notebook_link_is_found_with_slashes(self):
        self.assertEqual(ci.notebook_of(PAGE.replace("notebooks/", "notebooks%2F")),
                         "notebooks/source/saas-cdc/example-pipeline.html")

    def test_other_pages_keep_their_suffix(self):
        pages = ["https://x/ingestion/lakeflow-connect/example-pipeline",
                 "https://x/ingestion/lakeflow-connect/example-faq",
                 "https://x/ingestion/lakeflow-connect/other-faq"]
        out = ci.other_pages("example", pages)
        self.assertEqual(out, "[faq](https://x/ingestion/lakeflow-connect/example-faq) ")


if __name__ == "__main__":
    unittest.main()
