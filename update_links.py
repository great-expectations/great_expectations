import os
import pathlib
import re
from typing import Callable, Set


def update_file(
    directory: pathlib.Path,
    extentions: Set[str],
    update: Callable[[pathlib.Path, str], str],
) -> None:
    for root, dirs, files in os.walk(directory):
        for file in files:
            if any(file.endswith(ext) for ext in extentions):
                file_path = pathlib.Path(root) / file

                print(file_path)
                with file_path.open("r") as f:
                    content = f.read()

                updated_content = update(file_path, content)

                with file_path.open("w") as f:
                    f.write(updated_content)


def replace_absolute_html_links(
    directory: pathlib.Path,
    extentions: Set[str],
) -> None:
    def replace_it(file: pathlib.Path, content: str) -> str:
        href_pattern = re.compile(r"(?P<href>href=[\"\'])(?P<link>/docs/\S*[\"\'])")
        slug_pattern = re.compile(r"\nslug: (?P<slug>.+)\n")

        if slug_match := slug_pattern.search(content):
            # slugs override ultimate file location
            slug = slug_match.group("slug")
            slug = slug.replace("'", "")
            if slug[0] == "/":
                slug = slug[1:]
            absolute_style_dir = pathlib.Path("/docs/") / slug
            print("sdf")

        else:
            absolute_style_dir = pathlib.Path(
                f"/{file}".replace("/docs/docusaurus/", "/")
            ).parent

        return href_pattern.sub(
            lambda match: match.group("href")
            + str(
                pathlib.Path(match.group("link")).relative_to(
                    absolute_style_dir, walk_up=True
                )
            ),
            content,
        )

    return update_file(directory, extentions, replace_it)


def replace_absolute_md_links(
    directory: pathlib.Path,
    extentions: Set[str],
) -> None:
    def replace_it(file: pathlib.Path, content: str) -> str:
        link_pattern = re.compile(r"\]\(/docs/(?P<link>[^)]*)\)")

        def convert_to_file_link(link: str, original: str) -> str:
            if link.startswith("0.15.50/"):
                return original
            elif link.endswith(".png"):
                return original

            anchor = None
            if "#" in link:
                link, anchor = link.split("#")

            if link.endswith("/"):
                link = link[:-1]
            tries = [
                link,
                link + ".md",
                link + ".mdx",
            ]
            for try_link in tries:
                if (directory / try_link).exists():
                    if anchor:
                        return f"](/{try_link}#{anchor})"
                    else:
                        return f"](/{try_link})"
            return f"](<FIX ME>{original}</FIX ME>)"

        return link_pattern.sub(
            lambda match: convert_to_file_link(match.group("link"), match.group(0)),
            content,
        )

    return update_file(directory, extentions, replace_it)


def error_on_bad_img_links(
    directory: pathlib.Path,
    extentions: Set[str],
) -> None:
    def replace_it(file: pathlib.Path, content: str) -> str:
        if "../static/img/" in content:
            raise Exception(f"Bad img link in {file}")
        return content

    return update_file(directory, extentions, replace_it)


def replace_relative_links_to_docs(
    directory: pathlib.Path,
    extentions: Set[str],
) -> None:
    def replace_it(file: pathlib.Path, content: str) -> str:
        ...
        content = content.replace("../docs", "")
        content = content.replace("../version-0.17.23", "")
        content = content.replace("../version-0.16.16", "")
        content = content.replace("../version-0.15.50", "")
        content = content.replace("../version-0.14.13", "")

    return update_file(directory, extentions, replace_it)


def replace_absolute_imports(
    directory: pathlib.Path,
    extentions: Set[str],
) -> None:
    def replace_it(file: pathlib.Path, content: str) -> str:
        # relative_path = path_to_document.relative_to(path_to_versioned_docs)
        # dotted_relative_path = "/".join(".." for _ in range(len(relative_path.parts) - 1))
        pattern = re.compile(r"(?P<import>import .* from ')(?P<path>(@site)?/docs/.*)'")

        absolute_style_dir = pathlib.Path(
            f"/{file}".replace("/docs/docusaurus/", "/")
        ).parent

        def process_it(match: re.Match[str]):
            import_ = match.group("import")
            path = match.group("path")
            if path.startswith("@site"):
                path = path[5:]
            relative_path = str(
                pathlib.Path(path).relative_to(absolute_style_dir, walk_up=True)
            )
            if relative_path[0] != ".":
                relative_path = "./" + relative_path

            output = import_ + relative_path + "'"
            return output

        content = pattern.sub(
            lambda match: process_it(match),
            content,
        )
        return content

    return update_file(directory, extentions, replace_it)


if __name__ == "__main__":
    replace_absolute_html_links(pathlib.Path() / "docs/docusaurus/docs", {".jsx"})
    # replace_absolute_md_links(pathlib.Path() / "docs/docusaurus/docs", {".md", ".mdx"})
    # replace_relative_links_to_docs(
    #     pathlib.Path() / "docs/docusaurus/docs", {".js", ".jsx", ".md", ".mdx"}
    # )
    # error_on_bad_img_links(
    #     pathlib.Path() / "docs/docusaurus/docs", {".md", ".mdx", ".js", ".jsx"}
    # )
    replace_absolute_imports(pathlib.Path() / "docs/docusaurus/docs", {".md"})
    print("Don't forget to manually update react links with VersionedLink!!!")
    print("Replaced the easy stuff; time to manually check if anything else is up :(")
