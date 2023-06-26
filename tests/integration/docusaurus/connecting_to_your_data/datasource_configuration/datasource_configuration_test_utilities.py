import operator
from functools import reduce
from typing import List, Tuple

# The following method is used to ensure that the dictionary used to verify universal configuration elements
# remains the same in all the configuration tests.  Users may disregard it.


def _get_items_by_path(root_dictionary: dict, keys: Tuple[str]) -> Tuple:
    try:
        return "/".join(keys), reduce(operator.getitem, keys, root_dictionary)
    except:
        print(root_dictionary, keys)


def _gather_key_paths_from_dict(
    target_dict: dict, current_path: List[str] = None
) -> Tuple[List[Tuple[str]], List[str]]:
    key_paths: List[Tuple[str]] = []
    full_paths: List[Tuple[str]] = []
    for key, value in target_dict.items():
        if isinstance(value, dict):
            if current_path:
                next_path = current_path[:]
                full_paths.append(tuple(next_path))
                next_path.append(key)
                full_paths.append(tuple(next_path))
            else:
                next_path = [key]
            if value:
                new_key_paths, new_full_paths = _gather_key_paths_from_dict(
                    value, next_path
                )
                key_paths.extend(new_key_paths)
                full_paths.extend(new_full_paths)
            else:
                # If this is an empty dictionary, then there will be no further nested keys to gather.
                key_paths.append(tuple(next_path))
                full_paths.append(tuple(next_path))
        else:
            if current_path:
                next_path = current_path[:]
                next_path.append(key)
                key_paths.append(tuple(next_path))
                full_paths.append(tuple(next_path))
            else:
                full_paths.append((key,))
                key_paths.append((key,))
    return key_paths, full_paths


def is_subset(subset, superset, test_mode=True):
    superset_key_paths, superset_full_paths = _gather_key_paths_from_dict(superset)
    superset_full_paths = ["/".join(x) for x in superset_full_paths]
    # print(superset_key_paths)
    # print(superset_full_paths)
    # print("")

    subset_key_paths, subset_full_paths = _gather_key_paths_from_dict(subset)
    subset_full_paths = ["/".join(x) for x in subset_full_paths]
    # print(subset_key_paths)
    # print(subset_full_paths)
    # print("")

    subset_items = [
        _get_items_by_path(subset, key_path) for key_path in subset_key_paths
    ]
    superset_items = [
        _get_items_by_path(superset, key_path) for key_path in superset_key_paths
    ]
    # key_paths that point to empty values should only be checked to see if they exist as key_paths in the superset
    # empty_paths = [x[0] for x in subset_items if not x[1]]
    subset_items = [x for x in subset_items if x[1]]
    #
    # superset_items = [_get_items_by_path(superset, key_path) for key_path in key_paths]

    # Test that populated subset items correspond to items in the superset
    if subset_items:
        items_test = all(item in superset_items for item in subset_items)
    else:
        items_test = True

    # Test that the subset key paths correspond to key paths in the superset
    keys_test = all(item in superset_full_paths for item in subset_full_paths)

    if test_mode is True:
        assert all(
            [items_test, keys_test]
        ), f"\nEITHER:\n{subset_items}\n is not a subset of \n{superset_items}\n\nOR:\n\n{subset_full_paths}\n is not a subset of \n{superset_full_paths}"
    else:
        assert (
            all([items_test, keys_test]) is False
        ), f"\nBOTH:\n{subset_items} is a subset of \n{superset_items}\nAND:\n{subset_full_paths} is a subset of \n{superset_full_paths}"


if __name__ == "__main__":
    superset = {
        "test": "test",
        "test2": {"test3": "test3", "test4": {"test5": "test5"}},
    }

    # test empty top level subset:
    subset = {"test": "", "test2": {}}
    is_subset(subset, superset)

    # test correct top level subset:
    subset = {"test": "test", "test2": {}}
    is_subset(subset, superset)

    # test incorrect top level subset:
    subset = {"test": "test", "incorrect": {}}
    is_subset(subset, superset, False)

    # test incorrect top level subset:
    subset = {"test": "incorrect", "test2": {}}
    is_subset(subset, superset, False)

    ## Nested Tests:
    # test empty nested level 1 subsets:
    subset = {"test": "test", "test2": {"test3": "", "test4": {"test5": "test5"}}}
    is_subset(subset, superset)

    subset = {"test": "test", "test2": {"test3": "test3", "test4": {}}}
    is_subset(subset, superset)

    # test correct nested level 1 subsets:
    subset = {"test": "test", "test2": {"test3": "test3", "test4": {"test5": "test5"}}}
    is_subset(subset, superset)

    # test incorrect nested level 1 subsets:
    subset = {
        "test": "test",
        "test2": {"test3": "incorrect", "test4": {"test5": "test5"}},
    }
    is_subset(subset, superset, False)

    subset = {
        "test": "test",
        "test2": {"test3": "test3", "test4": {"incorrect": "test5"}},
    }
    is_subset(subset, superset, False)

    subset = {
        "test": "test",
        "test2": {"test3": "test3", "test4": {"test5": "incorrect"}},
    }
    is_subset(subset, superset, False)
