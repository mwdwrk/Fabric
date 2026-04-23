def compare_single_record_columns(
    tab_name_1,
    schema_1,
    tab_name_2,
    schema_2,
    compare_columns=None,
    ignore_columns=None,
    where=None,
):
    """
    Compare one logical record from two tables column-by-column and mark SAME/DIFF.

    Parameters:
    -----------
    tab_name_1 : str
        First table name (from HUBAA schema)
    schema_1 : str
        Schema name for table 1
    tab_name_2 : str
        Second table name (from UIOZIRN schema)
    schema_2 : str
        Schema name for table 2
    compare_columns : list, optional
        List of columns to compare (case-insensitive)
    ignore_columns : list, optional
        List of columns to exclude from comparison (case-insensitive)
    where : dict, mandatory
        Dictionary in format {"col1": "val1", "col2": 123}

    Returns:
    --------
    DataFrame
        DataFrame with *_t1, *_t2 and *_cmp columns for each compared column
    """

    def _dtype_is_string(dtype):
        d = (dtype or "").lower()
        return d.startswith("string") or d.startswith("varchar") or d.startswith("char")

    def _sql_literal(value):
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        return "'" + str(value).replace("'", "''") + "'"

    def _build_where_clause(where_dict, col_map):
        predicates = []
        for key, val in where_dict.items():
            key_upper = str(key).upper()
            if key_upper not in col_map:
                raise ValueError(f"WHERE column '{key}' not found in table")
            col_name = col_map[key_upper][0]
            if val is None:
                predicates.append(f"{col_name} IS NULL")
            else:
                predicates.append(f"{col_name} = {_sql_literal(val)}")
        return " AND ".join(predicates)

    compare_columns = compare_columns or []
    ignore_columns = ignore_columns or []

    if not isinstance(where, dict) or not where:
        raise ValueError("Parameter 'where' is mandatory and must be a non-empty dictionary")

    # Get metadata from both tables using existing helper from Cell 2
    cols_tab1 = get_table_columns(schema_1, tab_name_1)
    cols_tab2 = get_table_columns(schema_2, tab_name_2)

    if not cols_tab1 or not cols_tab2:
        raise ValueError("Unable to fetch columns from one or both tables")

    tab1_map = {c[0].upper(): (c[0], c[1]) for c in cols_tab1}
    tab2_map = {c[0].upper(): (c[0], c[1]) for c in cols_tab2}

    all_common_upper = sorted(set(tab1_map.keys()) & set(tab2_map.keys()))
    if not all_common_upper:
        raise ValueError("No common columns found between the two tables")

    compare_upper = [c.upper() for c in compare_columns]
    ignore_upper = {c.upper() for c in ignore_columns}

    # Selection logic based on requirements
    if compare_upper:
        missing_compare = [c for c in compare_upper if c not in tab1_map or c not in tab2_map]
        if missing_compare:
            raise ValueError(
                "Columns from compare_columns are missing in one of the tables: "
                + ", ".join(sorted(set(missing_compare)))
            )
        selected_upper = [c for c in compare_upper if c not in ignore_upper]
    elif ignore_upper:
        selected_upper = [c for c in all_common_upper if c not in ignore_upper]
    else:
        selected_upper = all_common_upper

    if not selected_upper:
        raise ValueError("No columns left to compare after applying compare/ignore filters")

    # WHERE keys are used to filter source tables and to join
    where_keys_upper = [str(k).upper() for k in where.keys()]
    for wk in where_keys_upper:
        if wk not in tab1_map or wk not in tab2_map:
            raise ValueError(f"WHERE key '{wk}' must exist in both tables")

    where_clause_t1 = _build_where_clause(where, tab1_map)
    where_clause_t2 = _build_where_clause(where, tab2_map)

    select_cols_t1 = ", ".join(tab1_map[c][0] for c in selected_upper)
    select_cols_t2 = ", ".join(tab2_map[c][0] for c in selected_upper)

    src_t1_name = f"{tab_name_1}_src_t1"
    src_t2_name = f"{tab_name_2}_src_t2"
    diff_name = f"{tab_name_1}_diff"

    create_src_t1_sql = f"""
    CREATE OR REPLACE TABLE {src_t1_name} AS
    SELECT {select_cols_t1}
    FROM {schema_1}.{tab_name_1}
    WHERE {where_clause_t1}
    """

    create_src_t2_sql = f"""
    CREATE OR REPLACE TABLE {src_t2_name} AS
    SELECT {select_cols_t2}
    FROM {schema_2}.{tab_name_2}
    WHERE {where_clause_t2}
    """

    join_on = " AND ".join(
        f"t1.{tab1_map[k][0]} = t2.{tab2_map[k][0]}" for k in where_keys_upper
    )

    diff_select_parts = []
    for c in selected_upper:
        col1, dtype1 = tab1_map[c]
        col2, dtype2 = tab2_map[c]

        left_cmp = f"trim(t1.{col1})" if _dtype_is_string(dtype1) else f"t1.{col1}"
        right_cmp = f"trim(t2.{col2})" if _dtype_is_string(dtype2) else f"t2.{col2}"

        diff_select_parts.append(f"t1.{col1} AS {col1}_t1")
        diff_select_parts.append(f"t2.{col2} AS {col2}_t2")
        diff_select_parts.append(
            "CASE "
            f"WHEN t1.{col1} IS NULL AND t2.{col2} IS NULL THEN 'SAME' "
            f"WHEN {left_cmp} = {right_cmp} THEN 'SAME' "
            "ELSE 'DIFF' END "
            f"AS {c}_cmp"
        )

    diff_select_sql = ",\n        ".join(diff_select_parts)

    create_diff_sql = f"""
    CREATE OR REPLACE TABLE {diff_name} AS
    SELECT
        {diff_select_sql}
    FROM {src_t1_name} t1
    INNER JOIN {src_t2_name} t2
        ON {join_on}
    """

    print("\n" + "=" * 80)
    print("GENERATED SQL - SOURCE TABLE 1")
    print("=" * 80)
    print(create_src_t1_sql)

    print("\n" + "=" * 80)
    print("GENERATED SQL - SOURCE TABLE 2")
    print("=" * 80)
    print(create_src_t2_sql)

    print("\n" + "=" * 80)
    print("GENERATED SQL - DIFF TABLE")
    print("=" * 80)
    print(create_diff_sql)
    print("=" * 80 + "\n")

    spark.sql(create_src_t1_sql)
    spark.sql(create_src_t2_sql)
    spark.sql(create_diff_sql)

    result_df = spark.table(diff_name)
    print(f"Created table {diff_name}. Rows: {result_df.count()}")

    return result_df



# Example: Compare the two tables with aliases and custom ordering

#     tab_name_1,
#     schema_1,
#     tab_name_2,
#     schema_2,
#     compare_columns=None,
#     ignore_columns=None,
#     where=None,


result = compare_single_record_columns(
    tab_name_1="agregat1_rw202512",
    schema_1="dbo",
    tab_name_2="agregat1_rw202512",
    schema_2="UIOZIRN",
    # tab_1_aliases={"login_uzytkownika_modyfikujacego": "login_uzytkownika_modyfikujacego"},
    # tab_2_aliases={"LOGINUZYTKOWNIKAMODYFIKUJACEGO": "login_uzytkownika_modyfikujacego"},
    ignore_columns=['kanal_sws_nazwa'],
    where={"ID_WERSJI_POLISY": 416382637, "ID_KONTRAKTU_TECH": 845424234}
    
)

# Display the results
display(result)
