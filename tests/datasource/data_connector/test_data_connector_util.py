import pytest

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.core.batch import BatchDefinition, BatchRequest, IDDict

# noinspection PyProtectedMember
from great_expectations.datasource.data_connector.util import (
    _invert_regex_to_data_reference_template,
    batch_definition_matches_batch_request,
    build_sorters_from_config,
    convert_batch_identifiers_to_data_reference_string_using_regex,
    convert_data_reference_string_to_batch_identifiers_using_regex,
    map_batch_definition_to_data_reference_string_using_regex,
    map_data_reference_string_to_batch_definition_list_using_regex,
)


def test_batch_definition_matches_batch_request():
    my_batch_definition = BatchDefinition(
        datasource_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
        batch_identifiers=IDDict(
            {"name": "eugene", "timestamp": "20200809", "price": "1500"}
        ),
    )

    # fully matching_batch_request
    my_batch_request = BatchRequest(
        datasource_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
        data_connector_query=None,
    )
    assert (
        batch_definition_matches_batch_request(my_batch_definition, my_batch_request)
        is True
    )

    # execution environment doesn't match
    my_batch_request = BatchRequest(
        datasource_name="i_dont_match",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
        data_connector_query=None,
    )
    assert (
        batch_definition_matches_batch_request(my_batch_definition, my_batch_request)
        is False
    )

    # data_connector_name doesn't match
    my_batch_request = BatchRequest(
        datasource_name="test_environment",
        data_connector_name="i_dont_match",
        data_asset_name="TestFiles",
        data_connector_query=None,
    )
    assert (
        batch_definition_matches_batch_request(my_batch_definition, my_batch_request)
        is False
    )

    # data_asset_name doesn't match
    my_batch_request = BatchRequest(
        datasource_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="i_dont_match",
        data_connector_query=None,
    )
    assert (
        batch_definition_matches_batch_request(my_batch_definition, my_batch_request)
        is False
    )

    # batch_request.data_connector_query.batch_filter_parameters is not dict
    my_batch_request = BatchRequest(
        datasource_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
        data_connector_query={"batch_filter_parameters": 1},
    )
    assert (
        batch_definition_matches_batch_request(my_batch_definition, my_batch_request)
        is False
    )

    # batch_identifiers do not match batch_definition.batch_identifiers
    my_batch_request = BatchRequest(
        datasource_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
        data_connector_query={"batch_filter_parameters": {"i": "wont_work"}},
    )
    assert (
        batch_definition_matches_batch_request(my_batch_definition, my_batch_request)
        is False
    )


def test_map_data_reference_string_to_batch_definition_list_using_regex():
    # regex_pattern does not match --> None
    data_reference = "alex_20200809_1000.csv"
    regex_pattern = r"^(.+)_____________\.csv$"
    group_names = ["name", "timestamp", "price"]
    returned_batch_def_list = (
        map_data_reference_string_to_batch_definition_list_using_regex(
            datasource_name="test_datasource",
            data_connector_name="test_data_connector",
            data_asset_name=None,
            data_reference=data_reference,
            regex_pattern=regex_pattern,
            group_names=group_names,
        )
    )
    assert returned_batch_def_list is None

    # no data_asset_name configured --> DEFAULT_ASSET_NAME
    data_reference = "alex_20200809_1000.csv"
    regex_pattern = r"^(.+)_(\d+)_(\d+)\.csv$"
    group_names = ["name", "timestamp", "price"]
    returned_batch_def_list = (
        map_data_reference_string_to_batch_definition_list_using_regex(
            datasource_name="test_datasource",
            data_connector_name="test_data_connector",
            data_asset_name=None,
            data_reference=data_reference,
            regex_pattern=regex_pattern,
            group_names=group_names,
        )
    )
    assert returned_batch_def_list == [
        BatchDefinition(
            datasource_name="test_datasource",
            data_connector_name="test_data_connector",
            data_asset_name="DEFAULT_ASSET_NAME",
            batch_identifiers=IDDict(
                {
                    "name": "alex",
                    "timestamp": "20200809",
                    "price": "1000",
                }
            ),
        )
    ]

    # data_asset_name configured
    returned_batch_def_list = (
        map_data_reference_string_to_batch_definition_list_using_regex(
            datasource_name="test_datasource",
            data_connector_name="test_data_connector",
            data_asset_name="test_data_asset",
            data_reference=data_reference,
            regex_pattern=regex_pattern,
            group_names=group_names,
        )
    )
    assert returned_batch_def_list == [
        BatchDefinition(
            datasource_name="test_datasource",
            data_connector_name="test_data_connector",
            data_asset_name="test_data_asset",
            batch_identifiers=IDDict(
                {
                    "name": "alex",
                    "timestamp": "20200809",
                    "price": "1000",
                }
            ),
        )
    ]


def test_convert_data_reference_string_to_batch_identifiers_using_regex():
    data_reference = "alex_20200809_1000.csv"
    pattern = r"^(.+)_(\d+)_(\d+)\.csv$"
    group_names = ["name", "timestamp", "price"]
    assert convert_data_reference_string_to_batch_identifiers_using_regex(
        data_reference=data_reference, regex_pattern=pattern, group_names=group_names
    ) == (
        "DEFAULT_ASSET_NAME",
        IDDict(
            {
                "name": "alex",
                "timestamp": "20200809",
                "price": "1000",
            }
        ),
    )

    data_reference = "eugene_20200810_1500.csv"
    pattern = r"^(.+)_(\d+)_(\d+)\.csv$"
    group_names = ["name", "timestamp", "price"]
    assert convert_data_reference_string_to_batch_identifiers_using_regex(
        data_reference=data_reference, regex_pattern=pattern, group_names=group_names
    ) == (
        "DEFAULT_ASSET_NAME",
        IDDict(
            {
                "name": "eugene",
                "timestamp": "20200810",
                "price": "1500",
            }
        ),
    )
    data_reference = "DOESNT_MATCH_CAPTURING_GROUPS.csv"
    pattern = r"^(.+)_(\d+)_(\d+)\.csv$"
    group_names = ["name", "timestamp", "price"]
    assert (
        convert_data_reference_string_to_batch_identifiers_using_regex(
            data_reference=data_reference,
            regex_pattern=pattern,
            group_names=group_names,
        )
        is None
    )

    data_reference = "eugene_DOESNT_MATCH_ALL_CAPTURING_GROUPS_1500.csv"
    pattern = r"^(.+)_(\d+)_(\d+)\.csv$"
    group_names = ["name", "timestamp", "price"]
    assert (
        convert_data_reference_string_to_batch_identifiers_using_regex(
            data_reference=data_reference,
            regex_pattern=pattern,
            group_names=group_names,
        )
        is None
    )


def test_map_batch_definition_to_data_reference_string_using_regex():
    # not BatchDefinition
    my_batch_definition = "I_am_a_string"
    group_names = ["name", "timestamp", "price"]
    regex_pattern = r"^(.+)_(\d+)_(\d+)\.csv$"
    with pytest.raises(TypeError):
        # noinspection PyUnusedLocal,PyTypeChecker
        my_data_reference = map_batch_definition_to_data_reference_string_using_regex(
            batch_definition=my_batch_definition,
            regex_pattern=regex_pattern,
            group_names=group_names,
        )

    # group names do not match
    my_batch_definition = BatchDefinition(
        datasource_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
        batch_identifiers=IDDict(
            {"name": "eugene", "timestamp": "20200809", "price": "1500"}
        ),
    )
    group_names = ["i", "wont", "match"]
    regex_pattern = r"^(.+)_(\d+)_(\d+)\.csv$"
    with pytest.raises(KeyError):
        my_data_reference = map_batch_definition_to_data_reference_string_using_regex(
            batch_definition=my_batch_definition,
            regex_pattern=regex_pattern,
            group_names=group_names,
        )

    # success
    my_batch_definition = BatchDefinition(
        datasource_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
        batch_identifiers=IDDict(
            {"name": "eugene", "timestamp": "20200809", "price": "1500"}
        ),
    )
    group_names = ["name", "timestamp", "price"]
    regex_pattern = r"^(.+)_(\d+)_(\d+)\.csv$"

    my_data_reference = map_batch_definition_to_data_reference_string_using_regex(
        batch_definition=my_batch_definition,
        regex_pattern=regex_pattern,
        group_names=group_names,
    )
    assert my_data_reference == "eugene_20200809_1500.csv"


def test_convert_batch_identifiers_to_data_reference_string_using_regex():
    pattern = r"^(.+)_(\d+)_(\d+)\.csv$"
    group_names = ["name", "timestamp", "price"]
    batch_identifiers = IDDict(
        **{
            "name": "alex",
            "timestamp": "20200809",
            "price": "1000",
        }
    )
    assert (
        convert_batch_identifiers_to_data_reference_string_using_regex(
            batch_identifiers=batch_identifiers,
            regex_pattern=pattern,
            group_names=group_names,
        )
        == "alex_20200809_1000.csv"
    )

    # Test an example with an uncaptured regex group (should return a WildcardDataReference)
    pattern = r"^(.+)_(\d+)_\d+\.csv$"
    group_names = ["name", "timestamp"]
    batch_identifiers = IDDict(
        **{
            "name": "alex",
            "timestamp": "20200809",
            "price": "1000",
        }
    )
    assert (
        convert_batch_identifiers_to_data_reference_string_using_regex(
            batch_identifiers=batch_identifiers,
            regex_pattern=pattern,
            group_names=group_names,
        )
        == "alex_20200809_*.csv"
    )

    # Test an example with an uncaptured regex group (should return a WildcardDataReference)
    pattern = r"^.+_(\d+)_(\d+)\.csv$"
    group_names = ["timestamp", "price"]
    batch_identifiers = IDDict(
        **{
            "name": "alex",
            "timestamp": "20200809",
            "price": "1000",
        }
    )
    assert (
        convert_batch_identifiers_to_data_reference_string_using_regex(
            batch_identifiers=batch_identifiers,
            regex_pattern=pattern,
            group_names=group_names,
        )
        == "*_20200809_1000.csv"
    )


# TODO: <Alex>Why does this method name have 2 underscores?</Alex>
def test__invert_regex_to_data_reference_template():
    returned = _invert_regex_to_data_reference_template(
        regex_pattern=r"^(.+)_(\d+)_(\d+)\.csv$",
        group_names=["name", "timestamp", "price"],
    )
    assert returned == "{name}_{timestamp}_{price}.csv"

    returned = _invert_regex_to_data_reference_template(
        regex_pattern=r"^(.+)_(\d+)_\d+\.csv$", group_names=["name", "timestamp"]
    )
    assert returned == "{name}_{timestamp}_*.csv"

    returned = _invert_regex_to_data_reference_template(
        regex_pattern=r"^.+_(\d+)_(\d+)\.csv$", group_names=["timestamp", "price"]
    )
    assert returned == "*_{timestamp}_{price}.csv"

    returned = _invert_regex_to_data_reference_template(
        regex_pattern=r"(^.+)_(\d+)_.\d\W\w[a-z](?!.*::.*::)\d\.csv$",
        group_names=["name", "timestamp"],
    )
    assert returned == "{name}_{timestamp}_*.csv"

    returned = _invert_regex_to_data_reference_template(
        regex_pattern=r"(.*)-([ABC])\.csv", group_names=["name", "type"]
    )
    assert returned == "{name}-{type}.csv"

    returned = _invert_regex_to_data_reference_template(
        regex_pattern=r"(.*)-[A|B|C]\.csv", group_names=["name"]
    )
    returned == "{name}-*.csv"

    # From https://github.com/madisonmay/CommonRegex/blob/master/commonregex.py
    date = r"(?:(?<!\:)(?<!\:\d)[0-3]?\d(?:st|nd|rd|th)?\s+(?:of\s+)?(?:jan\.?|january|feb\.?|february|mar\.?|march|apr\.?|april|may|jun\.?|june|jul\.?|july|aug\.?|august|sep\.?|september|oct\.?|october|nov\.?|november|dec\.?|december)|(?:jan\.?|january|feb\.?|february|mar\.?|march|apr\.?|april|may|jun\.?|june|jul\.?|july|aug\.?|august|sep\.?|september|oct\.?|october|nov\.?|november|dec\.?|december)\s+(?<!\:)(?<!\:\d)[0-3]?\d(?:st|nd|rd|th)?)(?:\,)?\s*(?:\d{4})?|[0-3]?\d[-\./][0-3]?\d[-\./]\d{2,4}"
    time = r"\d{1,2}:\d{2} ?(?:[ap]\.?m\.?)?|\d[ap]\.?m\.?"
    phone = r"""((?:(?<![\d-])(?:\+?\d{1,3}[-.\s*]?)?(?:\(?\d{3}\)?[-.\s*]?)?\d{3}[-.\s*]?\d{4}(?![\d-]))|(?:(?<![\d-])(?:(?:\(\+?\d{2}\))|(?:\+?\d{2}))\s*\d{2}\s*\d{3}\s*\d{4}(?![\d-])))"""
    phones_with_exts = r"((?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*(?:[2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|(?:[2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?(?:[2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?(?:[0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(?:\d+)?))"
    link = r'(?i)((?:https?://|www\d{0,3}[.])?[a-z0-9.\-]+[.](?:(?:international)|(?:construction)|(?:contractors)|(?:enterprises)|(?:photography)|(?:immobilien)|(?:management)|(?:technology)|(?:directory)|(?:education)|(?:equipment)|(?:institute)|(?:marketing)|(?:solutions)|(?:builders)|(?:clothing)|(?:computer)|(?:democrat)|(?:diamonds)|(?:graphics)|(?:holdings)|(?:lighting)|(?:plumbing)|(?:training)|(?:ventures)|(?:academy)|(?:careers)|(?:company)|(?:domains)|(?:florist)|(?:gallery)|(?:guitars)|(?:holiday)|(?:kitchen)|(?:recipes)|(?:shiksha)|(?:singles)|(?:support)|(?:systems)|(?:agency)|(?:berlin)|(?:camera)|(?:center)|(?:coffee)|(?:estate)|(?:kaufen)|(?:luxury)|(?:monash)|(?:museum)|(?:photos)|(?:repair)|(?:social)|(?:tattoo)|(?:travel)|(?:viajes)|(?:voyage)|(?:build)|(?:cheap)|(?:codes)|(?:dance)|(?:email)|(?:glass)|(?:house)|(?:ninja)|(?:photo)|(?:shoes)|(?:solar)|(?:today)|(?:aero)|(?:arpa)|(?:asia)|(?:bike)|(?:buzz)|(?:camp)|(?:club)|(?:coop)|(?:farm)|(?:gift)|(?:guru)|(?:info)|(?:jobs)|(?:kiwi)|(?:land)|(?:limo)|(?:link)|(?:menu)|(?:mobi)|(?:moda)|(?:name)|(?:pics)|(?:pink)|(?:post)|(?:rich)|(?:ruhr)|(?:sexy)|(?:tips)|(?:wang)|(?:wien)|(?:zone)|(?:biz)|(?:cab)|(?:cat)|(?:ceo)|(?:com)|(?:edu)|(?:gov)|(?:int)|(?:mil)|(?:net)|(?:onl)|(?:org)|(?:pro)|(?:red)|(?:tel)|(?:uno)|(?:xxx)|(?:ac)|(?:ad)|(?:ae)|(?:af)|(?:ag)|(?:ai)|(?:al)|(?:am)|(?:an)|(?:ao)|(?:aq)|(?:ar)|(?:as)|(?:at)|(?:au)|(?:aw)|(?:ax)|(?:az)|(?:ba)|(?:bb)|(?:bd)|(?:be)|(?:bf)|(?:bg)|(?:bh)|(?:bi)|(?:bj)|(?:bm)|(?:bn)|(?:bo)|(?:br)|(?:bs)|(?:bt)|(?:bv)|(?:bw)|(?:by)|(?:bz)|(?:ca)|(?:cc)|(?:cd)|(?:cf)|(?:cg)|(?:ch)|(?:ci)|(?:ck)|(?:cl)|(?:cm)|(?:cn)|(?:co)|(?:cr)|(?:cu)|(?:cv)|(?:cw)|(?:cx)|(?:cy)|(?:cz)|(?:de)|(?:dj)|(?:dk)|(?:dm)|(?:do)|(?:dz)|(?:ec)|(?:ee)|(?:eg)|(?:er)|(?:es)|(?:et)|(?:eu)|(?:fi)|(?:fj)|(?:fk)|(?:fm)|(?:fo)|(?:fr)|(?:ga)|(?:gb)|(?:gd)|(?:ge)|(?:gf)|(?:gg)|(?:gh)|(?:gi)|(?:gl)|(?:gm)|(?:gn)|(?:gp)|(?:gq)|(?:gr)|(?:gs)|(?:gt)|(?:gu)|(?:gw)|(?:gy)|(?:hk)|(?:hm)|(?:hn)|(?:hr)|(?:ht)|(?:hu)|(?:id)|(?:ie)|(?:il)|(?:im)|(?:in)|(?:io)|(?:iq)|(?:ir)|(?:is)|(?:it)|(?:je)|(?:jm)|(?:jo)|(?:jp)|(?:ke)|(?:kg)|(?:kh)|(?:ki)|(?:km)|(?:kn)|(?:kp)|(?:kr)|(?:kw)|(?:ky)|(?:kz)|(?:la)|(?:lb)|(?:lc)|(?:li)|(?:lk)|(?:lr)|(?:ls)|(?:lt)|(?:lu)|(?:lv)|(?:ly)|(?:ma)|(?:mc)|(?:md)|(?:me)|(?:mg)|(?:mh)|(?:mk)|(?:ml)|(?:mm)|(?:mn)|(?:mo)|(?:mp)|(?:mq)|(?:mr)|(?:ms)|(?:mt)|(?:mu)|(?:mv)|(?:mw)|(?:mx)|(?:my)|(?:mz)|(?:na)|(?:nc)|(?:ne)|(?:nf)|(?:ng)|(?:ni)|(?:nl)|(?:no)|(?:np)|(?:nr)|(?:nu)|(?:nz)|(?:om)|(?:pa)|(?:pe)|(?:pf)|(?:pg)|(?:ph)|(?:pk)|(?:pl)|(?:pm)|(?:pn)|(?:pr)|(?:ps)|(?:pt)|(?:pw)|(?:py)|(?:qa)|(?:re)|(?:ro)|(?:rs)|(?:ru)|(?:rw)|(?:sa)|(?:sb)|(?:sc)|(?:sd)|(?:se)|(?:sg)|(?:sh)|(?:si)|(?:sj)|(?:sk)|(?:sl)|(?:sm)|(?:sn)|(?:so)|(?:sr)|(?:st)|(?:su)|(?:sv)|(?:sx)|(?:sy)|(?:sz)|(?:tc)|(?:td)|(?:tf)|(?:tg)|(?:th)|(?:tj)|(?:tk)|(?:tl)|(?:tm)|(?:tn)|(?:to)|(?:tp)|(?:tr)|(?:tt)|(?:tv)|(?:tw)|(?:tz)|(?:ua)|(?:ug)|(?:uk)|(?:us)|(?:uy)|(?:uz)|(?:va)|(?:vc)|(?:ve)|(?:vg)|(?:vi)|(?:vn)|(?:vu)|(?:wf)|(?:ws)|(?:ye)|(?:yt)|(?:za)|(?:zm)|(?:zw))(?:/[^\s()<>]+[^\s`!()\[\]{};:\'".,<>?\xab\xbb\u201c\u201d\u2018\u2019])?)'
    email = r"([a-z0-9!#$%&'*+\/=?^_`{|.}~-]+@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)"
    ip = r"(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)"
    ipv6 = r"\s*(?!.*::.*::)(?:(?!:)|:(?=:))(?:[0-9a-f]{0,4}(?:(?<=::)|(?<!::):)){6}(?:[0-9a-f]{0,4}(?:(?<=::)|(?<!::):)[0-9a-f]{0,4}(?:(?<=::)|(?<!:)|(?<=:)(?<!::):)|(?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)(?:\.(?:25[0-4]|2[0-4]\d|1\d\d|[1-9]?\d)){3})\s*"
    price = r"[$]\s?[+-]?[0-9]{1,3}(?:(?:,?[0-9]{3}))*(?:\.[0-9]{1,2})?"
    hex_color = r"(#(?:[0-9a-fA-F]{8})|#(?:[0-9a-fA-F]{3}){1,2})\\b"
    credit_card = r"((?:(?:\\d{4}[- ]?){3}\\d{4}|\\d{15,16}))(?![\\d])"
    btc_address = r"(?<![a-km-zA-HJ-NP-Z0-9])[13][a-km-zA-HJ-NP-Z0-9]{26,33}(?![a-km-zA-HJ-NP-Z0-9])"
    street_address = r"\d{1,4} [\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\W?(?=\s|$)"
    zip_code = r"\b\d{5}(?:[-\s]\d{4})?\b"
    po_box = r"P\.? ?O\.? Box \d+"
    ssn = r"(?!000|666|333)0*(?:[0-6][0-9][0-9]|[0-7][0-6][0-9]|[0-7][0-7][0-2])[- ](?!00)[0-9]{2}[- ](?!0000)[0-9]{4}"

    regexes = {
        "dates": date,
        "times": time,
        "phones": phone,
        "phones_with_exts": phones_with_exts,
        "links": link,
        "emails": email,
        "ips": ip,
        "ipv6s": ipv6,
        "prices": price,
        "hex_colors": hex_color,
        "credit_cards": credit_card,
        "btc_addresses": btc_address,
        "street_addresses": street_address,
        "zip_codes": zip_code,
        "po_boxes": po_box,
        "ssn_number": ssn,
    }

    # This is a scattershot approach to making sure that our regex parsing has good coverage.
    # It does not guarantee perfect coverage of all regex patterns.
    for name, regex in regexes.items():
        print(name)
        group_names = ["name", "timestamp"]
        _invert_regex_to_data_reference_template(
            regex_pattern=regex, group_names=group_names
        )


def test_build_sorters_from_config_good_config():
    sorters_config = [
        {
            "orderby": "desc",
            "class_name": "NumericSorter",
            "name": "price",
        }
    ]
    sorters = build_sorters_from_config(sorters_config)
    assert sorters.__repr__() == str(
        "{'price': {'name': 'price', 'reverse': True, 'type': 'NumericSorter'}}"
    )
    assert sorters["price"].__repr__() == str(
        {"name": "price", "reverse": True, "type": "NumericSorter"}
    )
    # no sorters by name of i_dont_exist
    with pytest.raises(KeyError):
        sorters["i_dont_exist"]


def test_build_sorters_from_config_bad_config():
    # 1. class_name is bad
    sorters_config = [
        {
            "orderby": "desc",
            "class_name": "IDontExist",
            "name": "price",
        }
    ]
    with pytest.raises(ge_exceptions.PluginClassNotFoundError):
        build_sorters_from_config(sorters_config)

    # 2. orderby : not a real order
    sorters_config = [
        {
            "orderby": "not_a_real_order",
            "class_name": "NumericSorter",
            "name": "price",
        }
    ]
    with pytest.raises(ge_exceptions.SorterError):
        build_sorters_from_config(sorters_config)
