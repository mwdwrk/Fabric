def compare_single_record_by_column(
    tab_name_1,
    schema_1,
    tab_name_2,
    schema_2,
    where,
    tab_1_aliases=None,
    tab_2_aliases=None,
    compare_columns=None,
    ignore_columns=None
):
    """
    Compare one record from two tables column-by-column, returning one row per column.
    
    Parameters:
    -----------
    tab_name_1 : str
        First table name
    schema_1 : str
        Schema name for table 1
    tab_name_2 : str
        Second table name
    schema_2 : str
        Schema name for table 2
    where : dict, mandatory
        Dictionary in format {"col1": "val1", "col2": 123}
    tab_1_aliases : dict, optional
        Column aliases for table 1. Format: {"ORIGINAL_NAME": "common_name"}
    tab_2_aliases : dict, optional
        Column aliases for table 2. Format: {"ORIGINAL_NAME": "common_name"}
    compare_columns : list, optional
        List of columns to compare (uses common names from aliases)
    ignore_columns : list, optional
        List of column names to exclude (uses common names from aliases)
    
    Returns:
    --------
    DataFrame with columns: COL_NM, VAL_1, VAL_2, COMP (one row per column)
    """
    
    # Default empty structures
    tab_1_aliases = tab_1_aliases or {}
    tab_2_aliases = tab_2_aliases or {}
    compare_columns = compare_columns or []
    ignore_columns = ignore_columns or []
    
    if not where or not isinstance(where, dict):
        raise ValueError("Parameter 'where' is mandatory and must be a non-empty dictionary")
    
    # Fetch columns from both tables
    cols_tab1 = get_table_columns(schema_1, tab_name_1)
    cols_tab2 = get_table_columns(schema_2, tab_name_2)
    
    if not cols_tab1 or not cols_tab2:
        print("Error: Unable to fetch columns from one or both tables")
        return None
    
    # Build column maps: {UPPER_NAME: (original_name, data_type)}
    cols_tab1_map = {col[0].upper(): (col[0], col[1]) for col in cols_tab1}
    cols_tab2_map = {col[0].upper(): (col[0], col[1]) for col in cols_tab2}
    
    # Build alias maps: {UPPER_ORIGINAL: common_name}
    alias_map_1 = {k.upper(): v for k, v in tab_1_aliases.items()}
    alias_map_2 = {k.upper(): v for k, v in tab_2_aliases.items()}
    
    # Build reverse maps: {common_name: (original_name, data_type)} for each table
    common_to_t1 = {}
    for col_upper, (orig_name, dtype) in cols_tab1_map.items():
        common_name = alias_map_1.get(col_upper, orig_name)
        common_to_t1[common_name.upper()] = (orig_name, dtype)
    
    common_to_t2 = {}
    for col_upper, (orig_name, dtype) in cols_tab2_map.items():
        common_name = alias_map_2.get(col_upper, orig_name)
        common_to_t2[common_name.upper()] = (orig_name, dtype)
    
    # Find common columns (by common names)
    all_common = sorted(set(common_to_t1.keys()) & set(common_to_t2.keys()))
    
    if not all_common:
        print("Error: No common columns found between tables (after applying aliases)")
        return None
    
    # Apply compare_columns / ignore_columns logic
    compare_upper = [c.upper() for c in compare_columns]
    ignore_upper = {c.upper() for c in ignore_columns}
    
    if compare_upper:
        # Only use specified columns
        selected_common = [c for c in compare_upper if c in all_common]
        missing = [c for c in compare_upper if c not in all_common]
        if missing:
            print(f"⚠️  Warning: compare_columns not found in common columns: {', '.join(missing)}")
    else:
        # Use all common columns
        selected_common = all_common
    
    # Remove ignored columns
    selected_common = [c for c in selected_common if c not in ignore_upper]
    
    if not selected_common:
        print("Error: No columns left to compare after applying filters")
        return None
    
    print(f"📊 Comparing {len(selected_common)} column(s): {', '.join(selected_common)}")
    
    # Build WHERE clause for both tables
    def build_where(where_dict, col_map, alias_map):
        conditions = []
        for key, value in where_dict.items():
            key_upper = key.upper()
            # Try to find the column in the original table
            if key_upper in col_map:
                col_name = col_map[key_upper][0]
            else:
                # Maybe it's an aliased name, search in alias_map
                found = False
                for orig_upper, common_name in alias_map.items():
                    if common_name.upper() == key_upper and orig_upper in col_map:
                        col_name = col_map[orig_upper][0]
                        found = True
                        break
                if not found:
                    raise ValueError(f"WHERE column '{key}' not found in table")
            
            if value is None:
                conditions.append(f"{col_name} IS NULL")
            elif isinstance(value, str):
                conditions.append(f"{col_name} = '{value}'")
            else:
                conditions.append(f"{col_name} = {value}")
        
        return " AND ".join(conditions)
    
    where_clause_1 = build_where(where, cols_tab1_map, alias_map_1)
    where_clause_2 = build_where(where, cols_tab2_map, alias_map_2)
    
    # Build SELECT column lists for source tables
    select_cols_1 = ", ".join([common_to_t1[c][0] for c in selected_common])
    select_cols_2 = ", ".join([common_to_t2[c][0] for c in selected_common])
    
    # Temp table names (no schema)
    src_t1_name = f"{tab_name_1}_src_t1"
    src_t2_name = f"{tab_name_2}_src_t2"
    diff_table_name = f"{tab_name_1}_diff"
    
    # Generate CREATE TABLE statements for source tables
    create_src_t1 = f"""
CREATE TABLE {src_t1_name} AS
SELECT
    {select_cols_1}
FROM
    {schema_1}.{tab_name_1}
WHERE
    {where_clause_1}
"""
    
    create_src_t2 = f"""
CREATE TABLE {src_t2_name} AS
SELECT
    {select_cols_2}
FROM
    {schema_2}.{tab_name_2}
WHERE
    {where_clause_2}
"""
    
    # Build JOIN condition from WHERE keys
    join_conditions = []
    for key in where.keys():
        key_upper = key.upper()
        # Find column in both tables
        if key_upper in common_to_t1 and key_upper in common_to_t2:
            col1 = common_to_t1[key_upper][0]
            col2 = common_to_t2[key_upper][0]
        else:
            # Search by alias
            col1 = None
            col2 = None
            for orig_upper, common_name in alias_map_1.items():
                if common_name.upper() == key_upper:
                    col1 = cols_tab1_map[orig_upper][0]
                    break
            for orig_upper, common_name in alias_map_2.items():
                if common_name.upper() == key_upper:
                    col2 = cols_tab2_map[orig_upper][0]
                    break
            
            if not col1 or not col2:
                raise ValueError(f"WHERE key '{key}' must exist in both tables for JOIN")
        
        join_conditions.append(f"t1.{col1} = t2.{col2}")
    
    join_clause = " AND ".join(join_conditions)
    
    # Generate UNION ALL statements for each column
    union_parts = []
    for common_name in selected_common:
        col1_name, dtype1 = common_to_t1[common_name]
        col2_name, dtype2 = common_to_t2[common_name]
        
        # Determine if we need TRIM
        is_string1 = dtype1.lower().startswith('string') or dtype1.lower().startswith('varchar')
        is_string2 = dtype2.lower().startswith('string') or dtype2.lower().startswith('varchar')
        
        val1_expr = f"TRIM(t1.{col1_name})" if is_string1 else f"t1.{col1_name}"
        val2_expr = f"TRIM(t2.{col2_name})" if is_string2 else f"t2.{col2_name}"
        
        # Build comparison: handle NULLs as SAME
        comparison = f"""
    CASE 
        WHEN t1.{col1_name} IS NULL AND t2.{col2_name} IS NULL THEN 'SAME'
        WHEN {val1_expr} = {val2_expr} THEN 'SAME'
        ELSE 'DIFF'
    END AS COMP"""
        
        union_parts.append(f"""
SELECT
    '{common_name}' AS COL_NM,
    CAST(t1.{col1_name} AS STRING) AS VAL_1,
    CAST(t2.{col2_name} AS STRING) AS VAL_2,
    {comparison}
FROM
    {src_t1_name} t1
    INNER JOIN {src_t2_name} t2
    ON {join_clause}""")
    
    # Combine all UNION ALL parts
    create_diff_sql = f"""
CREATE TABLE {diff_table_name} AS
{' UNION ALL '.join(union_parts)}
"""
    
    # Print generated SQL
    print("\n" + "="*80)
    print("GENERATED SQL - SOURCE TABLE 1:")
    print("="*80)
    print(create_src_t1)
    
    print("\n" + "="*80)
    print("GENERATED SQL - SOURCE TABLE 2:")
    print("="*80)
    print(create_src_t2)
    
    print("\n" + "="*80)
    print("GENERATED SQL - DIFF TABLE:")
    print("="*80)
    print(create_diff_sql)
    print("="*80 + "\n")
    
    # Execute SQL
    print("Creating source table 1...")
    spark.sql(create_src_t1)
    
    print("Creating source table 2...")
    spark.sql(create_src_t2)
    
    print("Creating diff table...")
    spark.sql(create_diff_sql)
    
    # Return results
    result_df = spark.table(diff_table_name)
    row_count = result_df.count()
    print(f"\n✓ Comparison complete. Table '{diff_table_name}' created with {row_count} row(s).\n")
    
    return result_df
