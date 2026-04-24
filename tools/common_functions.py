### Common 

# Build column lists with TRIM for string columns
def build_column_list(schema, table_name, col_dict, aliases, common_only):
    """Build SQL column list with TRIM for string types and aliases"""
    result = []
    for col_upper in sorted(common_only):
        orig_name, data_type = col_dict[col_upper]
        
        # Check if column has an alias
        alias_target = aliases.get(orig_name, aliases.get(col_upper, None))
        
        # Wrap string columns with TRIM
        if data_type.lower().startswith('string') or data_type.lower().startswith('varchar'):
            col_expr = f"trim({orig_name})"
        else:
            col_expr = orig_name
        
        # Add alias if specified
        if alias_target:
            result.append(f"{col_expr} as {alias_target}")
        else:
            result.append(col_expr)
    
    return ','.join(result)


# Get column information for both tables
def get_table_columns(schema, table):
    """Fetch columns and their data types from table schema"""
    try:
        cols = spark.sql(f"DESCRIBE {schema}.{table}").collect()
        return [(row.col_name, row.data_type) for row in cols if row.col_name and not row.col_name.startswith('#')]
    except Exception as e:
        print(f"Error fetching columns from {schema}.{table}: {e}")
        return []
