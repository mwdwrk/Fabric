def compare_tables(tab_name_1,schema_1, tab_name_2,schema_2, tab_1_aliases=None, tab_2_aliases=None, order_columns=None, ignore_columns=None):
    """
    Compare two tables using SQL MINUS operations and return differences.
    
    Parameters:
    -----------
    tab_name_1 : str
        First table name (from HUBAA schema)
    tab_name_2 : str
        Second table name (from UIOZIRN schema)
    tab_1_aliases : dict, optional
        Column aliases for table 1. Format: {"ORIGINAL_NAME": "alias_name"}
    tab_2_aliases : dict, optional
        Column aliases for table 2. Format: {"ORIGINAL_NAME": "alias_name"}
    order_columns : list, optional
        List of columns to order by. SRC is always added as last column.
    ignore_columns : list, optional
        List of column names to exclude from comparison (case-insensitive)    
    Returns:
    --------
    DataFrame with differences between tables
    """
    import json


    # Default empty dicts if not provided
    tab_1_aliases = tab_1_aliases or {}
    tab_2_aliases = tab_2_aliases or {}
    order_columns = order_columns or []
    ignore_columns = ignore_columns or []
    
    # Get column information for both tables
    def get_table_columns(schema, table):
        """Fetch columns and their data types from table schema"""
        try:
            cols = spark.sql(f"DESCRIBE {schema}.{table}").collect()
            return [(row.col_name, row.data_type) for row in cols if row.col_name and not row.col_name.startswith('#')]
        except Exception as e:
            print(f"Error fetching columns from {schema}.{table}: {e}")
            return []
    
    # Fetch columns from both tables
    cols_tab1 = get_table_columns(schema_1, tab_name_1)
    cols_tab2 = get_table_columns(schema_2, tab_name_2)

    # Fetch row numbers from both tables

    
    if not cols_tab1 or not cols_tab2:
        print("Error: Unable to fetch columns from one or both tables")
        return None
    
    # Extract column names
    cols_tab1_names = {col[0].upper(): (col[0], col[1]) for col in cols_tab1}
    cols_tab2_names = {col[0].upper(): (col[0], col[1]) for col in cols_tab2}
    
    # Find common columns (case-insensitive)
    common_cols = set(cols_tab1_names.keys()) & set(cols_tab2_names.keys()) - set(col.upper() for col in ignore_columns)
    
    # Report removed columns
    removed_from_tab1 = set(cols_tab1_names.keys()) - common_cols
    removed_from_tab2 = set(cols_tab2_names.keys()) - common_cols
    
    if removed_from_tab1:
        print(f"⚠️  Columns in {tab_name_1} ({schema_1}) but not in {tab_name_2} ({schema_2}): {', '.join(sorted(removed_from_tab1))}")
    if removed_from_tab2:
        print(f"⚠️  Columns in {tab_name_2} ({schema_2}) but not in {tab_name_1} ({schema_1}): {', '.join(sorted(removed_from_tab2))}")
    
    if not common_cols:
        print("Error: No common columns found between tables")
        return None
    
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
    
    # Generate column lists
    col_list_1 = build_column_list(schema_1, tab_name_1, cols_tab1_names, tab_1_aliases, common_cols)
    col_list_2 = build_column_list(schema_2, tab_name_2, cols_tab2_names, tab_2_aliases, common_cols)
    
    # Build ORDER BY clause
    order_by_clause = ','.join(order_columns) + ',SRC' if order_columns else 'SRC'
    
    # Generate SQL
    sql_query = f"""
    select * from (
        select 
            'Fab' as src,{col_list_1}
        from
            {schema_1}.{tab_name_1}
        minus
        select 
            'Fab' as src,{col_list_2}
        from
            {schema_2}.{tab_name_2}

        union all

        select 
            'SAS' as src,{col_list_2}
        from
            {schema_2}.{tab_name_2}
        minus
        select 
            'SAS' as src,{col_list_1}
        from
            {schema_1}.{tab_name_1}
    ) tab
    order by {order_by_clause}
    """
    
    # Print generated SQL for review
    print("\n" + "="*80)
    print("GENERATED SQL:")
    print("="*80)
    print(sql_query)
    print("="*80 + "\n")
    # Row counts for tables
    row_cnt_1 = spark.table(f"{schema_1}.{tab_name_1}").count()
    row_cnt_2 = spark.table(f"{schema_2}.{tab_name_2}").count()
    print(f"Row numbers for {tab_name_1} ({schema_1}): {row_cnt_1}")
    print(f"Row numbers for {tab_name_2} ({schema_2}): {row_cnt_2}")
    print("="*80 + "\n")
    # Execute and return results
    print("Executing query...")
    result_df = spark.sql(sql_query)
    
    # Show count of differences
    diff_count = result_df.count()
    print(f"\n✓ Query completed. Found {diff_count} difference(s).\n")
    
    return result_df
