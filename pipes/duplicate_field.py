#!/usr/bin/env python3
from baseparser import BaseJSONPipe


class AddDuplicatedFieldPipe(BaseJSONPipe):
    def process_entry(self, entry):
        """Adds a duplicated field based on an existing field."""
        entry["duplicated_field"] = entry.get("convo_id", "")
        return entry


if __name__ == "__main__":
    parser = AddDuplicatedFieldPipe()
    parser.run()
