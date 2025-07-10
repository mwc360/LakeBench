def transpile_and_qualify_query(query:str, from_dialect:str, to_dialect:str, catalog:str, schema:str)-> str:
    import sqlglot as sg
    from sqlglot.optimizer.qualify_tables import qualify_tables
    expression = sg.parse_one(query, dialect=from_dialect)

    qualified_sql = qualify_tables(
        expression, 
        catalog=catalog, 
        db=schema, 
        dialect=from_dialect) \
    .sql(to_dialect, normalize=False)

    return qualified_sql