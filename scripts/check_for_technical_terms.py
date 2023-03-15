import json
import os
import re
from collections import namedtuple

Phrase = namedtuple("Phrase", "one_of_these but_not_in")
TODO = "- [ ]"
DONE = "- [X]"
NOTE = "-"

PHRASES = [
    Phrase(
        ("action",),
        (
            "abstraction",
            "fraction",
        ),
    ),
    Phrase(("batch",), ("batch request",)),
    Phrase(("batch request",), ()),
    Phrase(("catalog asset",), ()),
    Phrase(("checkpoint",), ("checkpoint store",)),
    Phrase(("checkpoint store",), ()),
    Phrase(("cli",), ("click",)),
    Phrase(("custom expectation",), ()),
    Phrase(("data asset",), ()),
    Phrase(("data connector",), ()),
    Phrase(("data context",), ()),
    Phrase(("data docs",), ("data docs store",)),
    Phrase(("data docs store",), ()),
    Phrase(("datasource",), ()),
    Phrase(
        ("evaluation parameter",),
        ("evaluation parameter store", "evaluation parameters store"),
    ),
    Phrase(("evaluation parameter store", "evaluation parameters store"), ()),
    Phrase(("execution engine",), ()),
    Phrase(
        ("expectation",),
        (
            "create expectation step",
            "expectation suite",
            "expectation store",
            "expectations store",
            "expectations suite",
            "great expectation",
            "custom expectation",
        ),
    ),
    Phrase(("expectation suite", "expectations suite", "suite"), ()),
    Phrase(("expectation store", "expectations store"), ()),
    Phrase(
        ("metric",),
        (
            "metric store",
            "parametrically",
        ),
    ),
    Phrase(("metric store",), ()),
    Phrase(("profiler",), ()),
    Phrase(("profiling",), ()),
    Phrase(("plugin",), ()),
    Phrase(("profile",), ("profiler",)),
    Phrase(("renderer",), ()),
    Phrase(
        ("store",),
        (
            "checkpoint store",
            "data docs store",
            "evaluation parameter store",
            "expectation store",
            "expectations store",
            "metric store",
            "validation result store",
            "stored",
            "validation results store",
            "can store",
            "will store",
            "S3 object stores",
        ),
    ),
    Phrase(("supporting resource",), ()),
    Phrase(
        ("validation", "validate"),
        (
            "validation result store",
            "validation results store",
            "validation result",
            "validate data step",
        ),
    ),
    Phrase(
        ("validation result",), ("validation result store", "validation results store")
    ),
    Phrase(("validation result store", "validation results store"), ()),
    Phrase(("validator",), ()),
]


def get_phrases(source_json_path):
    # Open the source_json_path
    with open(source_json_path) as json_file:
        # Read the contents as a json
        data = json.load(json_file)
        # Convert to list of tuples containing ("term", "definition", "url")
        data_list = [x["term"].lower() for x in data.values()]
        data_list.extend(PHRASES)
        # Order list alphabetically by "term"
        data_list.sort(key=lambda y: y[0])
        # return the ordered list.
        print(data_list)
        return data_list


def remove_but_not_in(line_working_contents, phrase):
    for _ in phrase.but_not_in:
        line_working_contents = line_working_contents.replace(_, "")
        line_working_contents = line_working_contents.replace(
            "_".join(_.split(" ")), ""
        )
    return line_working_contents


def phrase_in_line(line_working_contents, phrase):
    for possible_phrase in phrase.one_of_these:
        if possible_phrase in line_working_contents:
            return True
    return False


def phrase_is_tagged_in_line(line_working_contents, phrase):
    tagged_results = re.findall(
        "<TechnicalTag(.*?)\\>", line_working_contents, re.IGNORECASE
    )
    for possible_phrase in phrase.one_of_these:
        for tagged_result in tagged_results:
            if possible_phrase in tagged_result:
                return True
    return False


def is_header_line(line_working_contents):
    return line_working_contents.startswith("#")


def phrase_is_tagged_outside_generic_tag(line_working_contents, phrase):
    is_tagged = False
    is_present = False
    for find_tag in [r"<(.*?)>", r"\]\((.*?)\)", r"`+(\S*?)`+", r"\[(.*?)\]\("]:
        results = re.findall(find_tag, line_working_contents, re.IGNORECASE)
        for result in results:
            line_working_contents = line_working_contents.replace(result, "")
    if phrase_is_tagged_in_line(line_working_contents, phrase):
        is_tagged = True
    elif phrase_in_line(line_working_contents, phrase):
        is_present = True
    return is_tagged, is_present


def phrase_is_tagged_in_generic_tag(line_working_contents, phrase):
    is_tagged = False
    is_present = False
    for find_tag in [r"<(.*?)>", r"\]\((.*?)\)", r"`+(\S*?)`+", r"\[(.*?)\]\("]:
        results = re.findall(find_tag, line_working_contents, re.IGNORECASE)
        for result in results:
            if phrase_in_line(result, phrase):
                is_present = True
            if phrase_is_tagged_in_line(result, phrase):
                is_tagged = True
                break
        if is_tagged:
            break
    return is_tagged, is_present


def scan_file(file_path, phrases):  # noqa: C901 - complexity 21
    output = [file_path]
    with open(file_path) as active_file:
        for phrase in phrases:
            tag_found = False
            valid_lines = []
            header_lines = []
            code_lines = []
            generic_lines = []
            in_code_block = False
            in_title_block = False
            active_file.seek(0)
            for line_number, line_contents in enumerate(active_file, 1):
                line_working_contents = line_contents.lower().strip()
                line_working_contents = remove_but_not_in(line_working_contents, phrase)

                if line_working_contents.startswith("---") and (
                    line_number == 1 or in_title_block
                ):
                    in_title_block = not in_title_block
                if line_working_contents.startswith("title") and in_title_block:
                    header_lines.append(line_number)
                    continue

                if line_working_contents.startswith("```"):
                    in_code_block = not in_code_block

                if phrase_in_line(line_working_contents, phrase):
                    is_tagged = phrase_is_tagged_in_line(line_working_contents, phrase)
                    if is_tagged:
                        tag_found = True
                        valid_lines.append(f"TAG({line_number})")
                        continue
                    if in_code_block:
                        code_lines.append(
                            f"TAG({line_number})" if is_tagged else line_number
                        )
                    elif is_header_line(line_working_contents):
                        if is_tagged:
                            header_lines.append(f"TAG({line_number})")
                        else:
                            header_lines.append(line_number)
                    else:
                        tagged, present = phrase_is_tagged_outside_generic_tag(
                            line_working_contents, phrase
                        )
                        if tagged:
                            valid_lines.append(f"TAG({line_number})")
                        elif present:
                            valid_lines.append(line_number)
                        else:
                            tagged, present = phrase_is_tagged_in_generic_tag(
                                line_working_contents, phrase
                            )
                            if tagged:
                                generic_lines.append(f"TAG({line_number})")
                            elif present:
                                generic_lines.append(line_number)

                else:
                    # Phrase does not exist in line.
                    pass
            if len(valid_lines):
                output.append("")
                if tag_found:
                    output.append(f"- [X] {'/'.join(phrase.one_of_these)}")
                else:
                    output.append(f"- [ ] {'/'.join(phrase.one_of_these)}")
                output.append(f"    VALID: {valid_lines}")
                if header_lines:
                    output.append(f"   HEADER: {header_lines}")
                if generic_lines:
                    output.append(f"  GENERIC: {generic_lines}")
                if code_lines:
                    output.append(f"     CODE: {code_lines}")

        output.append("")
        output.append("------")
        output.append("")

        for note in output:
            print(note)


# def scan_file(file_path, phrases):
#     output = []
#     with open(file_path, "r") as a_file:
#         for phrase in phrases:
#             lines = []
#             tagged_lines = []
#             generic_lines = []
#             header_lines = []
#             valid_lines = []
#             found_first = False
#             in_code_block = False
#             is_done = False
#             a_file.seek(0)
#             for number, line in enumerate(a_file, 1):
#                 work_line = line.lower().strip()
#                 for not_this in phrase.but_not_in:
#                     work_line = work_line.replace(not_this, "")
#                     work_line = work_line.replace("_".join(not_this.split(" ")), "")
#                 if in_code_block:
#                     if work_line.startswith("```"):
#                         in_code_block = False
#                     continue
#                 for cur_phrase in phrase.one_of_these:
#                     if not in_code_block:
#                         if work_line.startswith("```"):
#                             in_code_block = True
#                             continue
#                         else:
#                             pass
#                     if scan_for_phrase(cur_phrase, work_line):
#                         if found_first:
#                             if work_line.startswith("#"):
#                                 if is_phrase_tagged(cur_phrase, work_line):
#                                     # text = f"\"{'/'.join(phrase.one_of_these)}\" is TAGGED IN A HEADER LINE."
#                                     # output.append((TODO, number, text))
#                                     header_lines.append(f"TAG:{number}")
#                                 else:
#                                     # text = f"\"{'/'.join(phrase.one_of_these)}\" is not tagged (header line)."
#                                     # output.append((NOTE, number, text))
#                                     header_lines.append(number)
#                             elif work_line.startswith("title:"):
#                                 if is_phrase_tagged(cur_phrase, work_line):
#                                     # text = f"\"{'/'.join(phrase.one_of_these)}\" is TAGGED IN THE TITLE BLOCK."
#                                     # output.append((TODO, number, text))
#                                     header_lines.append(f"TAG:{number}")
#                                 else:
#                                     text = f"\"{'/'.join(phrase.one_of_these)}\" is not tagged (title block)."
#                                     output.append((NOTE, number, text))
#                                     header_lines.append(number)
#                             else:
#                                 if is_phrase_tagged(cur_phrase, work_line):
#                                     # text = f"\"{'/'.join(phrase.one_of_these)}\" IS TAGGED AFTER FIRST OCCURENCE."
#                                     is_done = True
#                                     # output.append((TODO, number, text))
#                                     valid_lines.append(f"TAG({number})")
#                                 elif is_phrase_in_generic_tag(cur_phrase, work_line):
#                                     # text = f"\"{'/'.join(phrase.one_of_these)}\" is inside a generic tag."
#                                     # generic_lines.append(f"TAG({number})")
#                                     pass
#                                 else:
#                                     text = f"\"{'/'.join(phrase.one_of_these)}\" IS NOT TAGGED."
#                                     valid_lines.append(number)
#                                     lines.append(number)
#                                 break
#                         else:
#                             if work_line.startswith("#"):
#                                 if is_phrase_tagged(cur_phrase, work_line):
#                                     text = f"\"{'/'.join(phrase.one_of_these)}\" is TAGGED IN A HEADER LINE."
#                                     output.append((TODO, number, text))
#                                     header_lines.append(f"TAG:{number}")
#                                 else:
#                                     # text = f"\"{'/'.join(phrase.one_of_these)}\" is not tagged (header line)."
#                                     # output.append((NOTE, number, text))
#                                     header_lines.append(number)
#                             elif work_line.startswith("title:"):
#                                 if is_phrase_tagged(cur_phrase, work_line):
#                                     text = f"\"{'/'.join(phrase.one_of_these)}\" is TAGGED IN THE TITLE BLOCK."
#                                     output.append((TODO, number, text))
#                                     header_lines.append(f"TAG:{number}")
#                                 else:
#                                     # text = f"\"{'/'.join(phrase.one_of_these)}\" is not tagged (title block)."
#                                     header_lines.append(number)
#                                     # output.append((NOTE, number, text))
#                             else:
#                                 if is_phrase_tagged(cur_phrase, work_line):
#                                     text = f"\"{'/'.join(phrase.one_of_these)}\" is tagged."
#                                     is_done = True
#                                     found_first = True
#                                     output.append((DONE, number, text))
#                                     valid_lines.append(f"TAG:{number}")
#                                 elif is_phrase_in_generic_tag(cur_phrase, work_line):
#                                     # text = f"\"{'/'.join(phrase.one_of_these)}\" is inside a generic tag."
#                                     # output.append((NOTE, number, text))
#                                     generic_lines.append(number)
#                                 else:
#                                     text = f"\"{'/'.join(phrase.one_of_these)}\" IS NOT TAGGED."
#                                     lines.append(number)
#                                     # found_first = True
#                                     output.append((TODO, number, text))
#                                     valid_lines.append(f"TAG ME({number})")
#                                 break
#             if len(valid_lines) + len(generic_lines) + len(header_lines):
#                 output.append((f"", "", ""))
#                 output.append((f"{'/'.join(phrase.one_of_these)}", "", ""))
#                 output.append(("Valid lines:", valid_lines, f"{'/'.join(phrase.one_of_these)}"))
#                 output.append(("Header lines:", header_lines, f"{'/'.join(phrase.one_of_these)}"))
#                 output.append(("Generic lines:", generic_lines, f"{'/'.join(phrase.one_of_these)}"))
#             if len(lines) > 1 and not is_done:
#                 output.append((NOTE, lines[0], f"{'/'.join(phrase.one_of_these)} {lines}"))
#
#     return output
#
#
# def is_phrase_in_generic_tag(phrase, line):
#     def check(_find_tag):
#         results = re.findall(_find_tag, line, re.IGNORECASE)
#         for result in results:
#             if phrase in result:
#                 return True
#         return False
#
#     for find_tag in [
#         r'<(.*?)>',
#         r'\]\((.*?)\)',
#         r'`+(\S*?)`+',
#         r'\[(.*?)\]\('
#     ]:
#         results = re.findall(find_tag, line, re.IGNORECASE)
#         for result in results:
#             line = line.replace(result, "")
#
#     if check(find_tag):
#         return False
#     return True
#
#
# def is_phrase_tagged(phrase, line):
#     find_tag = r'<TechnicalTag(.*?)\\>'
#     results = re.findall(r'<TechnicalTag(.*?)/>', line, re.IGNORECASE)
#     for result in results:
#         if phrase in result:
#             return True
#     return False
#
#
# def scan_for_phrase(phrase, line):
#     return phrase in line
#
#
# def print_results(output):
#     # output.sort(key=lambda x:x[1])
#     for status, number, text in output:
#         print(status, number, text)
#
#
def iter_files(rootdir):
    # phrases = get_phrases(json_path)
    phrases = PHRASES[:]
    print(phrases)
    print("")
    print("------")
    print("")
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            # print os.path.join(subdir, file)
            filepath = subdir + os.sep + file
            if filepath.endswith(".md"):
                scan_file(filepath, phrases[:])
            else:
                continue


if __name__ == "__main__":
    # iter_files("../docs/guides/setup")
    # iter_files(
    #     "/Users/rachelreverie/PycharmProjects/great_expectations/docs/guides/expectations"
    # )
    filepath = "/Users/rachelreverie/PycharmProjects/great_expectations/ge_220811/docs/terms/data_assistant.md"
    scan_file(filepath, PHRASES[:])
    # rint_results(scan_file("../docs/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_to_postgresql.md", PHRASES))
    # print(is_phrase_in_generic_tag('store', ' **Confirm that the new Validation Results Store has been added by running** ``great_expectations store list``'))

    # phrase = Phrase(('expectation',), ('expectation suite', 'expectation store', 'expectations store', 'expectations suite', 'great expectation', 'custom expectation'))
    # line_contents = """We recommend that you create new <TechnicalTag relative="../../../" tag="data_context" text="Data Contexts" /> by using the a ``great_expectations init`` command in the directory where you want to deploy Great Expectations."""
    # line_working_contents = line_contents.lower().strip()
    # line_working_contents = remove_but_not_in(line_working_contents, phrase)
    # print(line_working_contents)
    # print(phrase_in_line(line_working_contents, phrase))
    # print(phrase_is_tagged_in_generic_tag(line_working_contents, phrase))
    # print(phrase_is_tagged_outside_generic_tag(line_working_contents, phrase))
