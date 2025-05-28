#!/usr/bin/env python3
"""
Script to revert migrations by generating DELETE commands based on a log of IDs.
Usage via command line:
    python migration_rollback.py log_21-05-2025_17-38-31_ids.txt
"""
import re
import sys
from collections import defaultdict

def parse_log_file(filename):
    """
    Reads the log file and returns a dict: {"schema.table": [id, ...]}.
    Lines that do not match the pattern will be ignored with a warning.
    """
    pattern = re.compile(r"^\[ID\]\s+(?P<table>\w+\.\w+)\.id:\s*(?P<id>\d+)\s*$")
    table_ids = defaultdict(list)

    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, start=1):
                match = pattern.match(line)
                if match:
                    table = match.group('table')
                    id_val = int(match.group('id'))
                    table_ids[table].append(id_val)
                else:
                    print(f"Aviso: ignorando linha {line_num} fora do padrao: {line.strip()}")
    except FileNotFoundError:
        print(f"Erro: arquivo '{filename}' nao encontrado.")
        sys.exit(1)

    # Sort and remove duplicates in each table
    for table, ids in table_ids.items():
        unique_sorted = sorted(set(ids))
        table_ids[table] = unique_sorted

    return table_ids


def group_into_ranges(ids):
    """
    Groups list of ids into consecutive ranges.
    Returns list of tuples (start, end).
    """
    ranges = []
    if not ids:
        return ranges

    start = prev = ids[0]
    for current in ids[1:]:
        if current == prev + 1:
            prev = current
        else:
            ranges.append((start, prev))
            start = prev = current
    ranges.append((start, prev))
    return ranges


def generate_delete_statements(table_ids):
    """
    Generates list of DELETE SQL commands from ranges by table.
    """
    statements = []
    for table, ids in table_ids.items():
        ranges = group_into_ranges(ids)
        for start, end in ranges:
            if start == end:
                stmt = f"DELETE FROM {table} WHERE id = {start};"
            else:
                stmt = f"DELETE FROM {table} WHERE id >= {start} AND id <= {end};"
            statements.append(stmt)
    return statements


def main():
    if len(sys.argv) != 2:
        print("Uso: python migration_rollback.py <arquivo_log.txt>")
        sys.exit(1)

    log_file = sys.argv[1]
    table_ids = parse_log_file(log_file)
    sql_statements = generate_delete_statements(table_ids)

    for stmt in sql_statements:
        print(stmt)

if __name__ == '__main__':
    main()