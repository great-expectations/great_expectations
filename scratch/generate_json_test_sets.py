import json
import random
from string import ascii_lowercase

def gen_random_row():
	return {
		"x": random.choice(ascii_lowercase),
		"y": random.randint(0,100),
		"z": random.normalvariate(0,1)
	}

json_list = [gen_random_row() for i in range(20)]
file('tests/test_sets/test_json_data_file.json', 'w').write(json.dumps(json_list, indent=2))

file('tests/test_sets/nested_test_json_data_file.json', 'w').write(json.dumps({
	"meta" : {"foo": "bar"},
	"other": {"baz": [1,2,3,4,5]},
	"data" : json_list
}, indent=2))