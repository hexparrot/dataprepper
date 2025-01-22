#!/usr/bin/env python3
from baseparser import BaseJSONParser


class AddDuplicatedFieldParser(BaseJSONParser):
    def process_entry(self, entry):
        """Adds a duplicated field based on an existing field."""
        entry["duplicated_field"] = entry.get("convo_id", "")
        return entry


if __name__ == "__main__":
    parser = AddDuplicatedFieldParser()
    parser.run()
