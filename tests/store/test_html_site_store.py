import pytest

from great_expectations.util import (
    gen_directory_tree_str,
)
from great_expectations.data_context.types import (
    ValidationResultIdentifier,
    ExpectationSuiteIdentifier,
    SiteSectionIdentifier,
)
from great_expectations.data_context.store import (
    HtmlSiteStore
)

def test_HtmlSiteStore(tmp_path_factory):

    path = str(tmp_path_factory.mktemp('test_HtmlSiteStore_with_FixedLengthTupleFileSystemStoreBackend__dir'))
    project_path = str(tmp_path_factory.mktemp('my_dir'))

    my_store = HtmlSiteStore(
        root_directory = path,
        base_directory = "my_store",
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    with pytest.raises(KeyError):
        my_store.get(ValidationResultIdentifier(**{}))
    
    ns_1 = SiteSectionIdentifier(
        site_section_name="my_site_section",
        resource_identifier=ValidationResultIdentifier(
            from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-100"
        )
    )
    my_store.set(ns_1, "aaa")
    # assert my_store.get(ns_1) == "aaa"

    ns_2 = SiteSectionIdentifier(
        site_section_name="my_site_section",
        resource_identifier=ValidationResultIdentifier(
            from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-20"
        )
    )
    my_store.set(ns_2, "bbb")
    # assert my_store.get(ns_2) == {"B": "bbb"}

    print(my_store.list_keys())
    assert set(my_store.list_keys()) == set([
        ns_1,
        ns_2,
    ])

    print(gen_directory_tree_str(path))
    assert gen_directory_tree_str(path) == """\
test_HtmlSiteStore_with_FixedLengthTupleFileSystemStoreBackend__dir0/
    my_store/
        prod-100/
            a/
                b/
                    c/
                        quarantine.html
        prod-20/
            a/
                b/
                    c/
                        quarantine.html
"""
