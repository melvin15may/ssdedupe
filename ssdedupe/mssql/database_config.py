import pymssql


def load_config(db={"database": "DATANEXUS_test", "host": "calv-sc-ctsidb.med.usc.edu", "password": "987654321spoj", "user": "MED\melvinm"}, table_name='test', config_table_name='config'):
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
    						"schema",
							"table",
							"key",
							filter_condition,
							prompt_for_labels,
							recall,
							training_file,
							seed
		 				FROM {config_table_name}
		 				WHERE "table" = '{table_name}' AND "key" is not null""".format(config_table_name=config_table_name, table_name=table_name))

    row = cursor.fetchone()
    if row is not None:
        row['prompt_for_labels'] = row['prompt_for_labels']
        if row['filter_condition'] is None or row['filter_condition'] == '':
            del row['filter_condition']
        if row['seed'] is None or row['seed'] == '':
            row['seed'] = 0
        config = row
    else:
        raise Exception("'{table_name}' not found in configuration table".format(
            table_name=table_name))

    # Get all fields
    cursor.execute("""
    	SELECT 
    		field,
    		field_type,
    		field_has_missing,
    		categories
    	FROM {config_table_name}
    	WHERE "table"= '{table_name}' AND "field" is not null
    """.format(config_table_name=config_table_name, table_name=table_name))

    fields = []
    categorical_fields = {}
    for row in cursor:
    	if row['field_has_missing'] is None or row['field_has_missing'] == '':
    		row['field_has_missing'] = False
    	row['type'] = row['field_type']
    	row['has_missing'] = row['field_has_missing']
    	del row['field_type']
    	del row['field_has_missing']
    	if row['type'] != 'Categorical':
    		fields.append(row)
    	else:
    		if row['field'] in categorical_fields:
    			categorical_fields[row['field']]['categories'].append(row['categories'])
    		else:
    			row['categories'] = [row['categories']]
    			categorical_fields[row['field']] = row

    config['fields'] = fields + list(categorical_fields.values())
    print(config)

    cursor.close()


load_config(table_name='PROJECT')
