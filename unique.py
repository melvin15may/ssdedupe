import pymssql
import sys
import collections
import os
import re
import json
import yaml
import click

sys.path.append(os.path.abspath('pgdedupe'))

PYTHON_VERSION = sys.version_info[0]

# Examples

config = {
	'strategy': 2,
	'table': 'PROJECT',
	'id': 'MasterID',
	'uniqueness_column': 'PREFERREDRECORD',
	'columns': ['Title', 'CTSIReview', 'ClinicalTrialStatus'],
	'conditions': {
		'Title': {
			'type': 'length',
			'weight': 45
		},
		'CTSIReview': {
			'type': 'length',
			'weight': 20
		},
		'ClinicalTrialStatus': {
			'type': 'regex',
			'weight': 35,
			'regex': '^yes$',
			'ignore_case': True #default: True
		}
	}
}


@click.command()
@click.option('--config',
              help='YAML- or JSON-formatted configuration file.',
              required=True)
@click.option('--db',
              help='YAML- or JSON-formatted database connection credentials.',
              required=True)
def main(config, db):

	dbconfig = load_config(db)
	config = load_config(config)
	
	del dbconfig['type']
	con = pymssql.connect(**dbconfig)
	config = process_config(config)

	c = con.cursor(as_dict=True)
  	c.execute("""
  		UPDATE {table} SET {uniqueness_column} = 1 WHERE dedupe_id in (
  			SELECT p.dedupe_id FROM {table} AS p GROUP BY p.dedupe_id HAVING count(p.dedupe_id) = 1
  		)
  		""".format(**config))
  	con.commit()
	c.execute("""SELECT {all_columns} 
		FROM {table}
		WHERE {uniqueness_column} = 0
		ORDER BY dedupe_id""".format(**config))

	table = []
	for row in c:
		table.append(unicode_to_str(row))

	initial_dedupe = None
	block_rows = []
	
	for row in table:
		if initial_dedupe is None:
			block_rows.append(row)
			initial_dedupe = row['dedupe_id']
		elif initial_dedupe == row['dedupe_id']:
			block_rows.append(row)
		else:
			strategy[config['strategy']](con, block_rows, config)
			block_rows = [row]
			initial_dedupe = row['dedupe_id']

	if len(block_rows) > 1:
		strategy[config['strategy']](con, block_rows, config)


def load_config(filename):
    ext = os.path.splitext(filename)[1].lower()
    with open(filename) as f:
        if ext == '.json':
            return json.load(f)
        elif ext in ('.yaml', '.yml'):
            return yaml.load(f)
        else:
            raise Exception('unknown filetype %s' % ext)


def process_config(config):
	arr = config['columns'] + [config['id']] + ['dedupe_id']
	config['all_columns'] = ', '.join(arr)
	return config

##### Strategy 1 functions START ##### 

def strategy_1(con, block_rows, config):
	c = con.cursor()
	greatest = block_rows[0]
	for row in block_rows[1:]:
		greatest = check_greatness(greatest, row, config['columns'])

	print greatest
	c.execute("""UPDATE {table} SET {uniqueness_column} = 1
		WHERE {id} = {greatest_id} 
		""".format(table=config['table'], id=config['id'], greatest_id=greatest[config['id']], uniqueness_column= config['uniqueness_column']))

	con.commit()

# returns the row with greater len
def check_greatness(r1, r2, columns):
	for c in columns:
		if r1[c] is None and r2[c] is not None:
			return r2
		elif r1[c] is not None and r2[c] is None:
			return r1
		elif r1[c] is not None and r2[c] is not None:
			if len(r1[c]) > len(r2[c]):
				return r1
			elif len(r1[c]) < len(r2[c]):
				return r2
	#Both are completely the same so return random
	return r1

#####  Strategy 1 functions END  ##### 

##### -------------------------- #####

##### Strategy 2 functions START ##### 

def strategy_2(con, block_rows, config):
	cur = con.cursor()
	greatest = block_rows[0]
	for c in config['columns']:
		if config['conditions'][c]['type'] == 'length':
			config['conditions'][c]['max_length'] = max([len(x[c]) if x[c] else 0 for x in block_rows])
	greatest_value = calculate_weight(block_rows[0], config)
	for row in block_rows[1:]:
		val = calculate_weight(row, config)
		if greatest_value < val:
			greatest_value = val
			greatest = row

	print greatest
	cur.execute("""UPDATE {table} SET {uniqueness_column} = 1
		WHERE {id} = {greatest_id} 
		""".format(table=config['table'], id=config['id'], greatest_id=greatest[config['id']], uniqueness_column= config['uniqueness_column']))

	con.commit()


def calculate_weight(row, config):
	score = 0
	for c in config['columns']:
		score += strategy_2_func[config['conditions'][c]['type']](value=row[c], 
																	weight=config['conditions'][c]['weight'], 
																	regex=config['conditions'][c].get('regex', None),
																	ignore_case=config['conditions'][c].get('ignore_case', True),
																	max_length=config['conditions'][c].get('max_length', None))
	return score

def compare_length(value, weight, max_length, regex=None, ignore_case=None):
	if value is not None:
		return len(value) * 1.0/max_length * weight
	else:
		return 0

def compare_regex(value, weight, regex, ignore_case, max_length=None):
	if value is None:
		return 0
	if ignore_case:
		pattern = re.compile(regex, flags=re.IGNORECASE)
	else:
		pattern = re.compile(regex)
	if pattern.match(value):
		return weight
	else:
		return 0

##### Strategy 2 functions END ##### 

def unicode_to_str(data):
    if data == "":
        data = None
    if PYTHON_VERSION < 3:
        if isinstance(data, basestring):
            #print data
            return data.encode('utf8')
            #return str(data)
        elif isinstance(data, collections.Mapping):
            return dict(map(unicode_to_str, data.iteritems()))
        elif isinstance(data, collections.Iterable):
            return type(data)(map(unicode_to_str, data))
        else:
            return data
    else:
        if isinstance(data, str):
            return data
        elif isinstance(data, collections.Mapping):
            return dict(map(unicode_to_str, data.items()))
        elif isinstance(data, collections.Iterable):
            return type(data)(map(unicode_to_str, data))
        else:
            return data

strategy = {
	1: strategy_1,
	2: strategy_2
}

strategy_2_func = {
	'length': compare_length,
	'regex': compare_regex
}

if __name__ == '__main__':
    main()
