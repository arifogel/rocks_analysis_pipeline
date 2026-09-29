"""Tests render_katydid_config's set-field rendering, independent of any real Katydid config."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import yaml

from rocks_analysis_pipeline.stage1_steps import render_katydid_config


class RenderKatydidConfigTest(unittest.TestCase):
    def test_writes_set_field_onto_the_matching_processor_instance(self) -> None:
        base_config = {
            "processor-toolbox": {
                "processors": [
                    {"type": "multi-band-event-builder", "name": "mbeb"},
                    {"type": "long-track-finder", "name": "ltf"},
                ],
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            base_path = Path(tmp) / "base.yaml"
            output_path = Path(tmp) / "rendered.yaml"
            with open(base_path, "w") as f:
                yaml.dump(base_config, f)

            render_katydid_config(str(base_path), 1.92223, output_path)

            with open(output_path) as f:
                rendered = yaml.load(f, Loader=yaml.FullLoader)

        self.assertEqual(rendered["mbeb"]["set-field"], 1.92223)
        self.assertNotIn("ltf", rendered)

    def test_raises_when_no_matching_processor_instance_exists(self) -> None:
        base_config = {
            "processor-toolbox": {
                "processors": [{"type": "long-track-finder", "name": "ltf"}],
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            base_path = Path(tmp) / "base.yaml"
            with open(base_path, "w") as f:
                yaml.dump(base_config, f)

            with self.assertRaises(ValueError):
                render_katydid_config(str(base_path), 1.92223, Path(tmp) / "rendered.yaml")


if __name__ == "__main__":
    unittest.main()
