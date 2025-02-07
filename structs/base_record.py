import json
from abc import ABC, abstractmethod
import sys


class BaseRecord(ABC):
    """
    Abstract base class to represent a record that will be serialized to JSON.
    Supports both in-memory and stdin-based parsing.
    """

    # Default required fields that must be non-null/non-empty
    DEFAULT_REQUIRED_FIELDS = ["author", "detail", "timestamp"]

    def __init__(self, required_fields=None):
        """
        :param required_fields: Optional list of additional fields required
                                by the subclass for a valid record.
        """
        if required_fields is None:
            required_fields = []

        # Merge default required fields with any subclass-specific required fields
        self.required_fields = list(self.DEFAULT_REQUIRED_FIELDS) + list(
            required_fields
        )

        # Holds all key-value data for this record
        self._fields = {}

    @abstractmethod
    def parse(self, raw_data):
        """
        Subclasses implement this method to parse raw_data and populate self._fields.
        Must set at least 'author', 'detail', and 'timestamp' (if they apply).
        """
        pass

    def set_field(self, key, value):
        """
        Set a field in this record.
        """
        self._fields[key] = value

    def get_field(self, key, default=None):
        """
        Retrieve a field from this record.
        """
        return self._fields.get(key, default)

    @property
    def is_valid(self):
        """
        Checks whether this record meets the minimum validity requirements.
        """
        for req_field in self.required_fields:
            val = self._fields.get(req_field, None)
            if val is None or (isinstance(val, str) and not val.strip()):
                return False
        return True

    def to_json(self):
        """
        Returns the record as a JSON object.
        """
        return json.dumps(self._fields, indent=2)

    @classmethod
    def from_stdin(cls):
        """
        Parses a record from stdin input (for standardized processing).
        """
        raw_content = sys.stdin.read().strip()
        instance = cls()
        instance.parse(raw_content)
        return instance

    def __str__(self):
        return self.to_json()
