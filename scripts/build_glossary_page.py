import json


def build_glossary_tuples(source_json_path):
    # Open the source_json_path
    with open(source_json_path) as json_file:
        # Read the contents as a json
        data = json.load(json_file)
        # Convert to list of tuples containing ("term", "definition", "url")
        data_list = [(x['term'], x['definition'], x['url']) for x in data.values()]
        # Order list alphabetically by "term"
        data_list.sort(key=lambda y: y[0])
        # return the ordered list.
        return data_list


def build_glossary_page(orderd_list_of_terms_tuples, glossary_file_path):
    # Open the glossary page for editing
    with open(glossary_file_path, "w") as glossary_file:
        # Write the glossary page header
        glossary_file.write("---\nid: glossary\ntitle: \"Glossary of Terms\"\n---\n\n")
        # iterate the glossary list of tuples and write glossary entries.
        for term, definition, url in orderd_list_of_terms_tuples:
            glossary_file.write(f"[**{term}:**](./{url}) {definition}\n\n")


def all_together_now(source_json_path, glossary_file_path):
    list_of_terms_tuples = build_glossary_tuples(source_json_path)
    build_glossary_page(list_of_terms_tuples, glossary_file_path)


if __name__ == '__main__':
    all_together_now(
        source_json_path="../docs/term_tags/terms.json",
        glossary_file_path="../docs/glossary.md"
    )
