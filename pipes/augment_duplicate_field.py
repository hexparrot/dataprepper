#!/usr/bin/env python3
import sys
from basepipe import BaseJSONPipe


class AddDuplicatedFieldPipe(BaseJSONPipe):
    """
    Duplicates a field in JSON entries.
    - Requires a source field and a new field name.
    - If the source field is missing in an entry, the duplicated field is set to an empty string.
    """

    def __init__(self, source_field, new_field_name, verbose=True):
        """
        :param source_field: The field to duplicate.
        :param new_field_name: The name of the duplicated field.
        :param verbose: Enable or disable logging to stderr.
        """
        if not source_field or not new_field_name:
            sys.stderr.write(
                "Error: Both source field and new field name are required.\n"
            )
            sys.exit(1)

        super().__init__(verbose)
        self.source_field = source_field
        self.new_field_name = new_field_name

    def process_entry(self, entry):
        """
        Duplicates the source field to the new field name.
        """
        entry[self.new_field_name] = entry.get(self.source_field, "")
        return entry


if __name__ == "__main__":
    # Ensure two required arguments are provided
    if len(sys.argv) < 3:
        sys.stderr.write(
            "Usage: cat somefile | ./augment_duplicate_field.py <field_to_duplicate> <new_field_name>\n"
        )
        sys.exit(1)

    source_field = sys.argv[1]
    new_field_name = sys.argv[2]

    parser = AddDuplicatedFieldPipe(source_field, new_field_name)
    parser.run()
