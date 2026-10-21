COLUMN_DESCRIPTION = "The column name."
COLUMN_A_DESCRIPTION = "The first column name."
COLUMN_B_DESCRIPTION = "The second column name."
COLUMN_LIST_DESCRIPTION = "Set of columns to be checked."
MOSTLY_DESCRIPTION = "Successful if at least `mostly` fraction of values match the Expectation."
IGNORE_ROW_IF_DESCRIPTION = (
    "If specified, sets the condition on which a given row is to be ignored."
)
VALUE_SET_DESCRIPTION = "A set of objects used for comparison."
WINDOWS_DESCRIPTION = "Definition(s) for evaluation of temporal windows"
FAILURE_SEVERITY_DESCRIPTION = (
    "The impact of this Expectation failing: critical, warning, or info. "
    "Defaults to critical if not set. "
    "Severity levels can be used to trigger different alerting patterns and actions."
)

LIKE_PATTERN_ESCAPE_DESCRIPTION = (
    "A single character that removes the special meaning of the `_` and `%` wildcards "
    "that follow it in the like pattern, emitted as a SQL `ESCAPE` clause. Required to "
    "match those characters literally: dialects disagree about an unannounced backslash "
    "(PostgreSQL treats it as an escape, SQLite treats it as an ordinary character, and "
    "Snowflake requires the clause to be stated), so a pattern relying on the default is "
    "not portable. Prefer a character other than a backslash: several dialects also treat "
    "a backslash specially inside string literals, before the pattern reaches LIKE, and "
    "Redshift rejects `ESCAPE '\\'` outright. Omit it to emit no `ESCAPE` clause. Not "
    "supported on BigQuery or ClickHouse, neither of which has an `ESCAPE` clause: escape "
    "wildcards with a backslash inside the pattern there instead."
)
