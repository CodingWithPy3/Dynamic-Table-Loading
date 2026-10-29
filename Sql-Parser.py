import re
from collections import OrderedDict


# ============================================================
# 1. SNOWFLAKE IDENTIFIER AND OPERATION REGEX PATTERNS
# ============================================================

IDENTIFIER = r'(?:[A-Za-z_][A-Za-z0-9_$]*|"(?:[^"]|"")*")'

# Supports TABLE, SCHEMA.TABLE, DATABASE.SCHEMA.TABLE,
# and quoted identifiers such as "DB"."SCHEMA"."TABLE".
TABLE_PATTERN = rf'{IDENTIFIER}(?:\s*\.\s*{IDENTIFIER}){{0,2}}'

TARGET_PATTERNS = {
    "CREATE_REPLACE": re.compile(
        rf'\bCREATE\s+OR\s+REPLACE\s+'
        rf'(?:(?:TEMP|TEMPORARY|TRANSIENT)\s+)?TABLE\s+'
        rf'(?:IF\s+NOT\s+EXISTS\s+)?({TABLE_PATTERN})',
        re.I
    ),
    "CREATE": re.compile(
        rf'\bCREATE\s+'
        rf'(?:(?:TEMP|TEMPORARY|TRANSIENT)\s+)?TABLE\s+'
        rf'(?:IF\s+NOT\s+EXISTS\s+)?({TABLE_PATTERN})',
        re.I
    ),
    "DROP": re.compile(
        rf'\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?({TABLE_PATTERN})',
        re.I
    ),
    "TRUNCATE": re.compile(
        rf'\bTRUNCATE\s+(?:TABLE\s+)?({TABLE_PATTERN})',
        re.I
    ),
    "DELETE": re.compile(
        rf'\bDELETE\s+FROM\s+({TABLE_PATTERN})',
        re.I
    ),
    "INSERT": re.compile(
        rf'\bINSERT\s+(?:OVERWRITE\s+)?INTO\s+({TABLE_PATTERN})',
        re.I
    ),
    "MERGE": re.compile(
        rf'\bMERGE\s+INTO\s+({TABLE_PATTERN})',
        re.I
    ),
    # Handles common UPDATE target syntax with an alias.
    "UPDATE": re.compile(
        rf'\bUPDATE\s+({TABLE_PATTERN})\s+'
        rf'(?:AS\s+)?[A-Za-z_][A-Za-z0-9_$]*\s+SET\b',
        re.I
    ),
}

SOURCE_PATTERN = re.compile(
    rf'\b(?:FROM|JOIN)\s+({TABLE_PATTERN})',
    re.I
)
MERGE_SOURCE_PATTERN = re.compile(
    rf'\bUSING\s+({TABLE_PATTERN})',
    re.I
)
CTE_PATTERN = re.compile(
    rf'(?:\bWITH|,)\s*({IDENTIFIER})\s+AS\s*\(',
    re.I
)


# ============================================================
# 2. REMOVE COMMENTS WITHOUT ALTERING QUOTED TEXT
# ============================================================

def remove_comments(sql):
    """Remove -- and /* */ comments outside quoted text."""
    output = []
    i = 0
    n = len(sql)

    while i < n:
        # Single-quoted string
        if sql[i] == "'":
            output.append(sql[i])
            i += 1

            while i < n:
                output.append(sql[i])

                if sql[i] == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        output.append(sql[i + 1])
                        i += 2
                        continue
                    i += 1
                    break

                i += 1

        # Double-quoted identifier
        elif sql[i] == '"':
            output.append(sql[i])
            i += 1

            while i < n:
                output.append(sql[i])

                if sql[i] == '"':
                    if i + 1 < n and sql[i + 1] == '"':
                        output.append(sql[i + 1])
                        i += 2
                        continue
                    i += 1
                    break

                i += 1

        elif sql.startswith("--", i):
            while i < n and sql[i] != "\n":
                i += 1

        elif sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            if end == -1:
                raise ValueError("Unclosed SQL block comment")
            output.append(" ")
            i = end + 2

        else:
            output.append(sql[i])
            i += 1

    return "".join(output)


# ============================================================
# 3. SPLIT STATEMENTS ON SEMICOLONS OUTSIDE QUOTED TEXT
# ============================================================

def split_statements(sql):
    """Split SQL statements while respecting quoted strings/IDs."""
    statements = []
    buffer = []
    quote = None
    i = 0

    while i < len(sql):
        char = sql[i]

        if quote:
            buffer.append(char)
            if char == quote:
                if i + 1 < len(sql) and sql[i + 1] == quote:
                    buffer.append(sql[i + 1])
                    i += 2
                    continue
                quote = None

        elif char in ("'", '"'):
            quote = char
            buffer.append(char)

        elif char == ";":
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []

        else:
            buffer.append(char)

        i += 1

    remaining = "".join(buffer).strip()
    if remaining:
        statements.append(remaining)

    return statements


# ============================================================
# 4. IDENTIFIER NORMALIZATION
# ============================================================

def normalize_identifier(identifier):
    """
    Unquoted Snowflake identifiers are normalized to uppercase.
    Quoted identifiers preserve case and unescape doubled quotes.
    """
    identifier = identifier.strip()

    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier[1:-1].replace('""', '"')

    return identifier.upper()


def split_table_name(raw_name):
    """Split a qualified table name without splitting quoted dots."""
    parts = []
    buffer = []
    quoted = False
    i = 0

    while i < len(raw_name):
        char = raw_name[i]

        if char == '"':
            buffer.append(char)

            if quoted and i + 1 < len(raw_name) and raw_name[i + 1] == '"':
                buffer.append(raw_name[i + 1])
                i += 2
                continue

            quoted = not quoted

        elif char == "." and not quoted:
            parts.append("".join(buffer).strip())
            buffer = []

        else:
            buffer.append(char)

        i += 1

    parts.append("".join(buffer).strip())

    if not 1 <= len(parts) <= 3:
        return None

    parts = [normalize_identifier(part) for part in parts]

    if len(parts) == 1:
        return {"database": None, "schema": None, "table": parts[0]}

    if len(parts) == 2:
        return {"database": None, "schema": parts[0], "table": parts[1]}

    return {"database": parts[0], "schema": parts[1], "table": parts[2]}


def canonical_name(table_info):
    """Build a consistent key from the available name components."""
    return ".".join(
        value
        for value in (
            table_info["database"],
            table_info["schema"],
            table_info["table"],
        )
        if value is not None
    )


# ============================================================
# 5. TARGET OPERATION DETECTION
# ============================================================

def detect_target(statement):
    """Return (mode, table_info) for a recognized target operation."""
    # CREATE_REPLACE must be checked before CREATE.
    for mode in (
        "CREATE_REPLACE", "MERGE", "CREATE", "DROP",
        "TRUNCATE", "DELETE", "INSERT", "UPDATE"
    ):
        match = TARGET_PATTERNS[mode].search(statement)
        if match:
            table_info = split_table_name(match.group(1))
            if table_info:
                return mode, table_info

    return None, None


# ============================================================
# 6. SOURCE TABLE EXTRACTION
# ============================================================

def extract_cte_names(statement):
    """Identify common CTE declarations in WITH clauses."""
    names = set()

    for match in CTE_PATTERN.finditer(statement):
        info = split_table_name(match.group(1))
        if info:
            names.add(canonical_name(info))

    return names


def extract_sources(statement, target_mode):
    """
    Extract sources from FROM/JOIN and MERGE USING clauses.
    Common CTE names are excluded from the source list.
    """
    cte_names = extract_cte_names(statement)
    sources = OrderedDict()

    for match in SOURCE_PATTERN.finditer(statement):
        info = split_table_name(match.group(1))
        if not info:
            continue

        key = canonical_name(info)
        if key not in cte_names:
            sources[key] = info

    if target_mode == "MERGE":
        match = MERGE_SOURCE_PATTERN.search(statement)
        if match:
            info = split_table_name(match.group(1))
            if info:
                key = canonical_name(info)
                if key not in cte_names:
                    sources[key] = info

    return list(sources.values())


# ============================================================
# 7. LOAD MODE RESOLUTION
# ============================================================

def update_load_patterns(record, current):
    """
    Preserve distinct load patterns for a target table.

    Examples:
      CREATE_REPLACE then INSERT -> ["CREATE-REPLACE", "INSERT"]
      DELETE then INSERT         -> ["DELETE-INSERT"]
      TRUNCATE then INSERT       -> ["DELETE-INSERT"]
      DROP then CREATE           -> ["DROP-CREATE"]

    Repeated instances of the same pattern are listed once.
    The internal last_operation field is used to detect paired
    operations across consecutive statements.
    """
    patterns = record["load_patterns"]
    previous = record.get("_last_operation")

    # Combine DROP + CREATE into a single load pattern.
    if previous == "DROP" and current in ("CREATE", "CREATE_REPLACE"):
        if patterns and patterns[-1] == "DROP":
            patterns.pop()
        combined = "DROP-CREATE"
        if combined not in patterns:
            patterns.append(combined)

    # Combine DELETE/TRUNCATE + INSERT into a single load pattern.
    elif previous in ("DELETE", "TRUNCATE") and current == "INSERT":
        if patterns and patterns[-1] in ("DELETE", "TRUNCATE"):
            patterns.pop()
        combined = "DELETE-INSERT"
        if combined not in patterns:
            patterns.append(combined)

    else:
        display_mode = {
            "CREATE_REPLACE": "CREATE-REPLACE",
            "CREATE": "CREATE",
            "DROP": "DROP",
            "TRUNCATE": "TRUNCATE",
            "DELETE": "DELETE",
            "INSERT": "INSERT",
            "MERGE": "MERGE",
            "UPDATE": "UPDATE",
        }.get(current, current)

        if display_mode not in patterns:
            patterns.append(display_mode)

    record["_last_operation"] = current


# ============================================================
# 8. MAIN PARSER
# ============================================================

def parse_sql(sql_text):
    """
    Parse SQL text and return one record per target table.

    Each record includes:
      database, schema, table, load_patterns, source_tables

    load_patterns preserves multiple distinct operations in order.
    For example, CREATE OR REPLACE followed by INSERT returns:
      ["CREATE-REPLACE", "INSERT"]

    This is a lightweight regex-based analyzer, not a complete
    Snowflake SQL grammar parser.
    """
    cleaned_sql = remove_comments(sql_text)
    statements = split_statements(cleaned_sql)
    results = OrderedDict()

    for statement in statements:
        mode, target = detect_target(statement)
        if not mode or not target:
            continue

        target_key = canonical_name(target)
        sources = extract_sources(statement, mode)

        if target_key not in results:
            results[target_key] = {
                **target,
                "load_patterns": [],
                "source_tables": OrderedDict(),
                "_last_operation": None,
            }

        record = results[target_key]
        update_load_patterns(record, mode)

        for source in sources:
            source_key = canonical_name(source)
            if source_key != target_key:
                record["source_tables"][source_key] = source

    final_results = []

    for record in results.values():
        # Remove internal parser state from public output.
        record.pop("_last_operation", None)
        record["source_tables"] = list(record["source_tables"].values())
        final_results.append(record)

    return final_results


# ============================================================
# 9. OPTIONAL QUICK TEST
# ============================================================

if __name__ == "__main__":
    sample_sql = """
    
    """

    for row in parse_sql(sample_sql):
        source_names = [
            canonical_name(source) for source in row["source_tables"]
        ]
        print({
            "database": row["database"],
            "schema": row["schema"],
            "table": row["table"],
            "load_patterns": row["load_patterns"],
            "source_tables": source_names,
        })


