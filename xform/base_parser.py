from abc import ABC, abstractmethod
import json


class BaseParser(ABC):
    """
    Abstract base class for structured text parsers.
    Provides a standard interface for parsing data and converting it to JSON.
    """

    def parse(self, raw_content: str) -> list[dict]:
        """
        Parses raw content into structured JSON format.
        :param raw_content: Raw data as a string.
        :return: List of dictionaries with cleaned and validated data.
        """
        sanitized_content = self._sanitize_content(raw_content)
        raw_records = self._extract_records(sanitized_content)
        return [record for record in map(self._normalize_record, raw_records)]

    def _sanitize_content(self, raw_content: str) -> str:
        """
        Optional sanitization step for input content.
        Removes invalid Unicode characters.
        """
        return raw_content.encode("utf-8", errors="ignore").decode("utf-8")

    @abstractmethod
    def _extract_records(self, raw_content: str) -> list[dict]:
        """
        Extracts structured records from raw content. Must be implemented by subclasses.
        """
        pass

    def _normalize_record(self, raw_record: dict) -> dict:
        """
        Cleans and standardizes record fields while retaining all original data.
        """
        return {
            key: value.strip() if isinstance(value, str) else value
            for key, value in raw_record.items()
        }


class StructuredParser(BaseParser):
    """
    A subclass of BaseParser for structured text formats such as CSV, JSON, TSV, etc.
    """

    @abstractmethod
    def _detect_format(self, raw_content: str) -> str:
        """
        Detects the format of the input data (e.g., CSV, TSV, JSON).
        Must be implemented by subclasses.
        """
        pass
