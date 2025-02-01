import csv
import sys
from xform.base_parser import BaseParser


class RedditCommentHeadersParser:
    """
    Parses comment_headers.csv to create a reference mapping.
    """

    @staticmethod
    def parse(file_path: str) -> dict:
        headers = {}
        with open(file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                headers[row["id"].strip()] = {
                    "permalink": row["permalink"].strip(),
                    "date": row["date"].strip(),
                }
        return headers


class RedditPostsHeadersParser:
    """
    Parses post_headers.csv to create a reference mapping.
    """

    @staticmethod
    def parse(file_path: str) -> dict:
        headers = {}
        with open(file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                headers[row["id"].strip()] = {
                    "permalink": row["permalink"].strip(),
                    "date": row["date"].strip(),
                }
        return headers


class RedditCommentsParser(BaseParser):
    """
    Parser for Reddit comments.
    """

    def __init__(self, username: str, comment_headers: dict):
        self.username = username
        self.comment_headers = comment_headers

    def _extract_records(self, file_path: str) -> list[dict]:
        records = []
        with open(file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    timestamp = row["date"].strip()
                    message = row["body"].strip()
                    comment_id = row["id"].strip()

                    reference = self.comment_headers.get(comment_id, {}).get(
                        "permalink", ""
                    )
                    reference_date = self.comment_headers.get(comment_id, {}).get(
                        "date", "1970-01-01 00:00:00 UTC"
                    )

                    record = {
                        "timestamp": timestamp,
                        "message": message,
                        "type": "comment",
                        "author": self.username,
                        "product": "reddit",
                        "reference": reference,
                        "reference_date": reference_date,
                    }
                    records.append(record)
                except Exception as e:
                    print(f"[ERROR] Failed to parse comment: {e}", file=sys.stderr)
                    continue
        return records


class RedditPostsParser(BaseParser):
    """
    Parser for Reddit posts.
    """

    def __init__(self, username: str, post_headers: dict):
        self.username = username
        self.post_headers = post_headers

    def _extract_records(self, file_path: str) -> list[dict]:
        records = []
        with open(file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    timestamp = row["date"].strip()
                    message = row["title"].strip()
                    post_id = row["id"].strip()

                    reference = self.post_headers.get(post_id, {}).get("permalink", "")
                    reference_date = self.post_headers.get(post_id, {}).get(
                        "date", "1970-01-01 00:00:00 UTC"
                    )

                    record = {
                        "timestamp": timestamp,
                        "message": message,
                        "type": "post",
                        "author": self.username,
                        "product": "reddit",
                        "reference": reference,
                        "reference_date": reference_date,
                    }
                    records.append(record)
                except Exception as e:
                    print(f"[ERROR] Failed to parse post: {e}", file=sys.stderr)
                    continue
        return records


class RedditStatisticsParser:
    """
    Parses the statistics.csv file to extract the Reddit username.
    """

    @staticmethod
    def parse(file_path: str) -> str:
        with open(file_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if row[0].strip().lower() == "account name":
                    return row[1].strip()
        return "unspecified"
