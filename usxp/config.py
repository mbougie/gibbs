"""
Run a function from a JSON configuration file.
"""

import json


def from_config(func):
	"""Run a function from a JSON configuration file."""
	
	def decorator(filename):
		with open(filename, 'r') as file_in:
			config = json.load(file_in)

		#'**' takes a dict and extracts its contents and passes them as parameters to a function.
		#returns the intial function with new arguments????
		return func(**config)
	
	## return the decorated input function
	return decorator
			

