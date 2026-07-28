"""
Validation for CSV responses from the Wikidata SPARQL endpoint.
"""

import csv
import collections
import io
import logging
import re
from typing import Dict, NamedTuple, Optional, Sequence

log = logging.getLogger(__name__)

# _check_structure catches every corrupted file on its own; these are a failsafe
# that also names the cause outright.
ERROR_MARKERS = (
    "SPARQL-QUERY: queryStr=",
    "java.util.concurrent.TimeoutException",
    "com.bigdata.rdf.sail.webapp",
    "org.eclipse.jetty",
)

ID_PATTERNS = {
    "imdbId":   re.compile(r"tt\d{7,9}"),
    "tvdbId":   re.compile(r"[1-9]\d{0,8}"),
    "tmdbId":   re.compile(r"[1-9]\d{0,8}"),
    "tvmazeId": re.compile(r"[1-9]\d{0,8}"),
}

REQUIRED_COLUMNS = ("imdbId", "tmdbId")


class CsvValidationError(Exception):
    """Raised when a SPARQL CSV response is incomplete or malformed."""


class SanitizeResult(NamedTuple):
    """Outcome of :func:`sanitize_ids`."""
    csv: str
    kept: int
    dropped: int
    blanked: int
    recovered: int
    reasons: Dict[str, int]     # "<column> (junk value|recovered)" -> count


def validate_sparql_csv(text: str, expected_columns: Sequence[str]) -> int:
    """Validates a response body and returns its row count, raising if it is
    empty, carries a server error trailer, or is not well-formed CSV."""
    if not text or not text.strip():
        raise CsvValidationError("Response body is empty.")

    _reject_error_trailer(text)
    row_count = _check_structure(text, expected_columns)

    log.info(f"CSV validation passed: {row_count} rows, {len(expected_columns)} columns.")
    return row_count


def _reject_error_trailer(text: str) -> None:
    """Looks for the Java error report Blazegraph appends on a query timeout."""
    for marker in ERROR_MARKERS:
        index = text.find(marker)
        if index != -1:
            line = text.count("\n", 0, index) + 1
            raise CsvValidationError(
                f"Server error trailer found at line {line} (marker: {marker!r}). "
                f"The query timed out mid-stream and the result set is truncated."
            )


def _check_structure(text: str, expected_columns: Sequence[str]) -> int:
    """Parses the body strictly and returns the data row count."""
    reader = csv.reader(io.StringIO(text, newline=""))

    try:
        header = next(reader)
    except StopIteration:
        raise CsvValidationError("Response contains no header row.") from None

    if header != list(expected_columns):
        raise CsvValidationError(
            f"Unexpected header: got {header}, expected {list(expected_columns)}."
        )

    width = len(expected_columns)
    row_count = 0
    try:
        for row in reader:
            if len(row) != width:
                raise CsvValidationError(
                    f"Malformed row at line {reader.line_num}: expected {width} "
                    f"fields, got {len(row)} ({row[:4]!r})."
                )
            row_count += 1
    except csv.Error as e:
        raise CsvValidationError(f"CSV parse failed at line {reader.line_num}: {e}") from e

    if row_count == 0:
        raise CsvValidationError("Response contains a header but no data rows.")

    return row_count


def is_valid_id(column: str, value: str) -> bool:
    """Reports whether a value is a well-formed ID for its column. Empty values
    and columns with no known format are accepted, so a query gaining a field
    does not start silently discarding data."""
    if not value:
        return True

    pattern = ID_PATTERNS.get(column)
    if pattern is None:
        return True

    return pattern.fullmatch(value) is not None


def recover_id(column: str, value: str) -> Optional[str]:
    """Strips a trailing URL slug from an otherwise intact ID, as in
    45522/code-lyoko-evolution or 449294-radd.
    """
    head = re.split(r"[/-]", value, maxsplit=1)[0]

    if not head or head == value:
        return None

    return head if is_valid_id(column, head) else None


def sanitize_ids(text: str) -> SanitizeResult:
    """Removes values that are not valid IDs for their provider.
    A malformed value is first offered to recover_id. What cannot be recovered
    is discarded.
    """
    reader = csv.reader(io.StringIO(text, newline=""))
    header = next(reader)

    required = {c for c in REQUIRED_COLUMNS if c in header}
    out = io.StringIO(newline="")
    # CRLF matches the SPARQL serializer, so a body with nothing to clean
    # round-trips byte for byte.
    writer = csv.writer(out, lineterminator="\r\n")
    writer.writerow(header)

    kept = dropped = blanked = recovered = 0
    reasons: Dict[str, int] = collections.Counter()
    samples: Dict[str, str] = {}

    def note(key: str, sample: str) -> None:
        reasons[key] += 1
        samples.setdefault(key, sample)

    for row in reader:
        cleaned = list(row)
        drop = False

        for i, column in enumerate(header):
            value = row[i]
            if is_valid_id(column, value):
                continue

            repaired = recover_id(column, value)
            if repaired is not None:
                cleaned[i] = repaired
                recovered += 1
                note(f"{column} (recovered)", f"{value} -> {repaired}")
                continue

            note(f"{column} (junk value)", value)

            if column in required:
                drop = True
                break

            cleaned[i] = ""
            blanked += 1

        if drop:
            dropped += 1
            continue

        writer.writerow(cleaned)
        kept += 1

    if dropped or blanked or recovered:
        log.warning(f"Cleaned IDs: {dropped} row(s) dropped, {blanked} field(s) "
                    f"blanked, {recovered} field(s) recovered, {kept} rows kept.")
        for key, count in sorted(reasons.items(), key=lambda kv: -kv[1]):
            log.warning(f"  {count} x {key}, e.g. {samples[key]!r}")
    else:
        log.info(f"No junk IDs found in {kept} rows.")

    return SanitizeResult(out.getvalue(), kept, dropped, blanked, recovered, dict(reasons))
