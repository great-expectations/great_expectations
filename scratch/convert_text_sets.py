import json
J = json.load(file('tests/test_sets/expect_column_values_to_be_between_test_set.json'))
J
for j in J:
    print(j)
for j in J:
    j['in'] = {
        'column' : j['in'][0],
        'min_value' : j['in'][1],
        'max_value' : j['in'][2],
    }
J
file('tests/test_sets/expect_column_values_to_be_between_test_set_ADJ.json', 'w').write(json.dumps(J, indent=2))
