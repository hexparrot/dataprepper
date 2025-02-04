import json
from abc import ABC, abstractmethod


class BaseRecord(ABC):
    """
    Abstract base class to represent a record that will be serialized to JSON.

    Subclasses must implement the 'parse' method to populate _fields.
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
        :param key: Name of the field (string).
        :param value: Value of the field.
        """
        self._fields[key] = value

    def get_field(self, key, default=None):
        """
        Retrieve a field from this record.
        :param key: Name of the field (string).
        :param default: Returned if the field isn't found.
        """
        return self._fields.get(key, default)

    @property
    def is_valid(self):
        """
        Checks whether this record meets the minimum validity requirements:
        - All required fields are present.
        - All required fields are non-null and non-empty (if they're strings).
        """
        for req_field in self.required_fields:
            val = self._fields.get(req_field, None)
            if val is None:
                return False
            if isinstance(val, str) and not val.strip():
                # if it's a string, it must not be empty
                return False
        return True

    def __str__(self):
        """
        Returns a pretty-printed JSON string representing this record
        (flat, single-level). Indentation ensures it's human-readable and
        easily navigated by parsers (like ijson).
        """
        return json.dumps(self._fields, indent=2)
