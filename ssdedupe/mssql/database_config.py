import pymssql


def load_config(db, table_name, config_table_name='config'):
    """
    Load configuration from a table
    db: database credentials
    table_name: name of table to be deduplicated
    config_table_name : name of configuration table (default: 'config')
    """
    con = pymssql.connect(**db)
    cursor = con.cursor(as_dict=True)

    config = {}

    # Get basic details from table
    cursor.execute("""SELECT 
    						schema,
							table,
							key,
							filter_condition,
							prompt_for_labels,
							recall,
							training_file,
							seed
		 				FROM {config_table_name}
		 				WHERE table == '{table_name}'""".format(config_table_name=config_table_name, table_name=table_name))

    row = cursor.fetchone()
    if row is not None:
        config = row
        row['prompt_for_labels'] = True if row['prompt_for_labels'] == 1 else False
    else:
        raise Exception("'{table_name}' not found in configuration table".format(
            table_name=table_name))

    cursor.close()
