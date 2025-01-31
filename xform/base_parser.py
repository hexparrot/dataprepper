from abc import ABC, abstractmethod
from datetime import datetime


class BaseParser(ABC):
    """
    Abstract base class for parsers.
    Ensures a consistent interface and JSON output format, with unified validation.
    """

    def parse(self, html_content: str) -> list[dict]:
        """
        Parse HTML content into the standardized JSON structure.
        :param html_content: Raw HTML content as a string.
        :return: List of dictionaries with cleaned and validated data.
        """
        sanitized_content = self._sanitize_content(html_content)
        raw_records = self._extract_records(sanitized_content)
        return [
            record
            for record in map(self._normalize_record, raw_records)
            if self._validate_record(record)
        ]

    def _sanitize_content(self, html_content: str) -> str:
        """
        Sanitize the input HTML content by removing invalid Unicode characters.
        :param html_content: Raw HTML content as a string.
        :return: Sanitized HTML content.
        """
        return html_content.encode("ascii", errors="ignore").decode("ascii")

    @abstractmethod
    def _extract_records(self, html_content: str) -> list[dict]:
        """
        Extract raw records from HTML content. Must be implemented by subclasses.
        :param html_content: Sanitized HTML content as a string.
        :return: List of dictionaries with parsed fields.
        """
        pass

    def _normalize_record(self, raw_record: dict) -> dict:
        """
        Cleans string fields but retains all fields present in the original record.
        :param raw_record: Dictionary with raw extracted fields.
        :return: Cleaned dictionary with all original fields retained.
        """
        return {
            key: (
                self._format_timestamp(value) if key == "timestamp" else value.strip()
            )
            if isinstance(value, str)
            else value
            for key, value in raw_record.items()
        }

    def _validate_record(self, record: dict) -> bool:
        """
        Validate that a record has all required fields.
        :param record: A JSON record.
        :return: True if valid, False otherwise.
        """
        # Ensure 'author', 'message', and 'timestamp' are non-empty
        required_fields = ["author", "message", "timestamp"]
        return all(record.get(field) for field in required_fields)

    def _format_timestamp(self, raw_timestamp: str) -> str:
        """
        Format raw timestamp into ISO 8601 format.
        :param raw_timestamp: Raw timestamp string.
        :return: Formatted ISO 8601 timestamp or the original string if formatting fails.
        """
        try:
            datetime_obj = datetime.strptime(raw_timestamp, "%Y-%m-%d %H:%M:%S")
            return datetime_obj.isoformat()
        except ValueError:
            return raw_timestamp  # Return as-is if formatting fails
