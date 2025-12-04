import os

import pandas as pd

absolute_path = os.path.dirname(os.path.abspath(__file__))
file_path = absolute_path + "/data_description.csv"
# load your description file
df = pd.read_csv(file_path)

# find nested roots
nested_roots = df[df["type"] == "Nested"]["name"].tolist()

# build mapping: root -> [(sub_name, sub_type), ...]
nested = {}
for root in nested_roots:
    sub = df[df["name"].str.startswith(root + ".")]
    nested[root] = list(zip(
        sub["name"].str.split(".").str[1],
        sub["type"]
    ))

# collect simple (non-nested) columns
nested_sub_names = {
    f"{root}.{subname}"
    for root, subs in nested.items()
    for subname, _ in subs
}

simple_cols = []
for _, row in df.iterrows():
    name = row["name"]
    typ = row["type"]

    if typ == "Nested":
        continue
    if name in nested_sub_names:
        continue

    simple_cols.append((name, typ))

# now emit the DDL
lines = []
lines.append("CREATE DATABASE IF NOT EXISTS github_log;")
lines.append("")
lines.append("USE github_log;")
lines.append("")
lines.append("CREATE TABLE IF NOT EXISTS year2020")
lines.append("(")

col_lines = []

# scalar columns
for name, typ in simple_cols:
    col_lines.append(f"    {name} {typ},")

# nested columns, multi-line for readability
for root, subs in nested.items():
    col_lines.append(f"    {root} Nested(")
    for i, (subname, subtyp) in enumerate(subs):
        comma = "," if i < len(subs) - 1 else ""
        col_lines.append(f"        {subname} {subtyp}{comma}")
    col_lines.append("    ),")

# remove trailing comma from last column definition
col_lines[-1] = col_lines[-1].rstrip(",")

lines.extend(col_lines)
lines.append(")")
lines.append("ENGINE = MergeTree")
lines.append("PARTITION BY created_date")
lines.append("ORDER BY (created_at, repo_id, id);")

ddl = "\n".join(lines)
print(ddl)
