import glob
from pprint import pprint

from scripts.dependency_graph_testing.parse import (
    parse_declaration_nodes,
    parse_import_nodes,
    parse_pytest_fixtures,
    parse_tests_dependencies,
)


def test_parse_declaration_nodes():
    """
    What does this test and why?

    Ensures that all declaration statements for classes and functions are included in
    the 'declaration_map'. If there is a namespace collision (i.e. two modules defining
    the same name class/function), the map should include BOTH in the form of a list).
    """
    root_dir = "great_expectations"
    declaration_map = parse_declaration_nodes(root_dir)
    core_concepts = [
        # Classes
        ("DataContext", ["great_expectations/data_context/data_context.py"]),
        ("Checkpoint", ["great_expectations/checkpoint/checkpoint.py"]),
        (
            "Validator",
            [
                "great_expectations/validator/validator.py",
                "great_expectations/marshmallow__shade/validate.py",
            ],
        ),
        # Functions
        ("instantiate_class_from_config", ["great_expectations/data_context/util.py"]),
        (
            "suite_new",
            ["great_expectations/cli/suite.py", "great_expectations/cli/v012/suite.py"],
        ),
        (
            "get_validator",
            [
                "great_expectations/rule_based_profiler/util.py",
                "great_expectations/cli/toolkit.py",
            ],
        ),
    ]

    for concept, locations in core_concepts:
        assert declaration_map.get(concept) == locations


def test_parse_import_nodes():
    pass

    # declaration_map = parse_declaration_nodes("great_expectations")
    # dependency_graph = parse_import_nodes("great_expectations", declaration_map)

    # files = [f for f in glob.glob("great_expectations/**/*.py", recursive=True)]
    # assert len(files) == len(dependency_graph)

    # for key, value in dependency_graph.items():
    #     assert key.startswith("great_expectations")
    #     for v in value:
    #         assert v.startswith("great_expectations")

    # pprint(dependency_graph) # 415
    # assert False


def test_parse_pytest_fixtures():
    pass


def test_parse_tests_dependencies():
    pass
