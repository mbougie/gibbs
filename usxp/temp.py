
import sys
import json
import psycopg2


def populate():
	"""Build all the arguments for a specific task."""
	params = {
		'x': 1,
		'y': 2,
		'z': 3,
		'w': 4
	}
	return params


def nibble(x, y, **kwargs):
	return kwargs


def populate_scratch_params(dsn):
	with psycopg2.connect(dsn) as conn:
		cur = conn.cursor()
		cur.execute("""
		INSERT INTO series.scratch_params(doc)
		VALUES (%(doc)s)
		""", {'doc': json.dumps({'x': 7, 'y': 9, 'years': [2015, 2016]})})
		conn.commit()
	return 'completed'


def from_database(func):

	def decorator(id_, dsn="dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'"):
		with psycopg2.connect(dsn) as conn:
			params = 


if __name__ == '__main__':
	# print nibble(hey='Hey!', **populate())
	print populate_scratch_params("dbname='usxp' user='mbougie' host='144.92.235.105' password='Mend0ta!'")