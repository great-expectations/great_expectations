"Git implementation of _version.py."
import errno
import os
import re
import subprocess
import sys


def get_keywords():
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Get the keywords needed to look up the version information."
    git_refnames = "$Format:%d$"
    git_full = "$Format:%H$"
    git_date = "$Format:%ci$"
    keywords = {"refnames": git_refnames, "full": git_full, "date": git_date}
    return keywords


class VersioneerConfig:
    "Container for Versioneer configuration parameters."


def get_config():
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Create, populate and return the VersioneerConfig() object."
    cfg = VersioneerConfig()
    cfg.VCS = "git"
    cfg.style = "pep440"
    cfg.tag_prefix = ""
    cfg.parentdir_prefix = "great_expectations-"
    cfg.versionfile_source = "great_expectations/_version.py"
    cfg.verbose = False
    return cfg


class NotThisMethod(Exception):
    "Exception raised if a method is not valid for the current scenario."


LONG_VERSION_PY = {}
HANDLERS = {}


def register_vcs_handler(vcs, method):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Decorator to mark a method as the handler for a particular VCS."

    def decorate(f):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Store f in HANDLERS[vcs][method]."
        if vcs not in HANDLERS:
            HANDLERS[vcs] = {}
        HANDLERS[vcs][method] = f
        return f

    return decorate


def run_command(commands, args, cwd=None, verbose=False, hide_stderr=False, env=None):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Call the given command(s)."
    assert isinstance(commands, list)
    for c in commands:
        try:
            dispcmd = str([c] + args)
            p = subprocess.Popen(
                ([c] + args),
                cwd=cwd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=(subprocess.PIPE if hide_stderr else None),
            )
            break
        except OSError:
            e = sys.exc_info()[1]
            if e.errno == errno.ENOENT:
                continue
            if verbose:
                print(f"unable to run {dispcmd}")
                print(e)
            return (None, None)
    else:
        if verbose:
            print(f"unable to find command, tried {commands}")
        return (None, None)
    stdout = p.communicate()[0].strip()
    if sys.version_info[0] >= 3:
        stdout = stdout.decode()
    if p.returncode != 0:
        if verbose:
            print(f"unable to run {dispcmd} (error)")
            print(f"stdout was {stdout}")
        return (None, p.returncode)
    return (stdout, p.returncode)


def versions_from_parentdir(parentdir_prefix, root, verbose):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Try to determine the version from the parent directory name.\n\n    Source tarballs conventionally unpack into a directory that includes both\n    the project name and a version string. We will also support searching up\n    two directory levels for an appropriately named parent directory\n    "
    rootdirs = []
    for i in range(3):
        dirname = os.path.basename(root)
        if dirname.startswith(parentdir_prefix):
            return {
                "version": dirname[len(parentdir_prefix) :],
                "full-revisionid": None,
                "dirty": False,
                "error": None,
                "date": None,
            }
        else:
            rootdirs.append(root)
            root = os.path.dirname(root)
    if verbose:
        print(
            "Tried directories %s but none started with prefix %s"
            % (str(rootdirs), parentdir_prefix)
        )
    raise NotThisMethod("rootdir doesn't start with parentdir_prefix")


@register_vcs_handler("git", "get_keywords")
def git_get_keywords(versionfile_abs):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Extract version information from the given file."
    keywords = {}
    try:
        f = open(versionfile_abs)
        for line in f.readlines():
            if line.strip().startswith("git_refnames ="):
                mo = re.search('=\\s*"(.*)"', line)
                if mo:
                    keywords["refnames"] = mo.group(1)
            if line.strip().startswith("git_full ="):
                mo = re.search('=\\s*"(.*)"', line)
                if mo:
                    keywords["full"] = mo.group(1)
            if line.strip().startswith("git_date ="):
                mo = re.search('=\\s*"(.*)"', line)
                if mo:
                    keywords["date"] = mo.group(1)
        f.close()
    except OSError:
        pass
    return keywords


@register_vcs_handler("git", "keywords")
def git_versions_from_keywords(keywords, tag_prefix, verbose):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Get version information from git keywords."
    if not keywords:
        raise NotThisMethod("no keywords at all, weird")
    date = keywords.get("date")
    if date is not None:
        date = date.strip().replace(" ", "T", 1).replace(" ", "", 1)
    refnames = keywords["refnames"].strip()
    if refnames.startswith("$Format"):
        if verbose:
            print("keywords are unexpanded, not using")
        raise NotThisMethod("unexpanded keywords, not a git-archive tarball")
    refs = {r.strip() for r in refnames.strip("()").split(",")}
    TAG = "tag: "
    tags = {r[len(TAG) :] for r in refs if r.startswith(TAG)}
    if not tags:
        tags = {r for r in refs if re.search("\\d", r)}
        if verbose:
            print(f"discarding '{','.join((refs - tags))}', no digits")
    if verbose:
        print(f"likely tags: {','.join(sorted(tags))}")
    for ref in sorted(tags):
        if ref.startswith(tag_prefix):
            r = ref[len(tag_prefix) :]
            if verbose:
                print(f"picking {r}")
            return {
                "version": r,
                "full-revisionid": keywords["full"].strip(),
                "dirty": False,
                "error": None,
                "date": date,
            }
    if verbose:
        print("no suitable tags, using unknown + full revision id")
    return {
        "version": "0+unknown",
        "full-revisionid": keywords["full"].strip(),
        "dirty": False,
        "error": "no suitable tags",
        "date": None,
    }


@register_vcs_handler("git", "pieces_from_vcs")
def git_pieces_from_vcs(tag_prefix, root, verbose, run_command=run_command):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Get version from 'git describe' in the root of the source tree.\n\n    This only gets called if the git-archive 'subst' keywords were *not*\n    expanded, and _version.py hasn't already been rewritten with a short\n    version string, meaning we're inside a checked out source tree.\n    "
    GITS = ["git"]
    if sys.platform == "win32":
        GITS = ["git.cmd", "git.exe"]
    (out, rc) = run_command(
        GITS, ["rev-parse", "--git-dir"], cwd=root, hide_stderr=True
    )
    if rc != 0:
        if verbose:
            print(f"Directory {root} not under git control")
        raise NotThisMethod("'git rev-parse --git-dir' returned error")
    (describe_out, rc) = run_command(
        GITS,
        [
            "describe",
            "--tags",
            "--dirty",
            "--always",
            "--long",
            "--match",
            f"{tag_prefix}*",
        ],
        cwd=root,
    )
    if describe_out is None:
        raise NotThisMethod("'git describe' failed")
    describe_out = describe_out.strip()
    (full_out, rc) = run_command(GITS, ["rev-parse", "HEAD"], cwd=root)
    if full_out is None:
        raise NotThisMethod("'git rev-parse' failed")
    full_out = full_out.strip()
    pieces = {}
    pieces["long"] = full_out
    pieces["short"] = full_out[:7]
    pieces["error"] = None
    git_describe = describe_out
    dirty = git_describe.endswith("-dirty")
    pieces["dirty"] = dirty
    if dirty:
        git_describe = git_describe[: git_describe.rindex("-dirty")]
    if "-" in git_describe:
        mo = re.search("^(.+)-(\\d+)-g([0-9a-f]+)$", git_describe)
        if not mo:
            pieces["error"] = f"unable to parse git-describe output: '{describe_out}'"
            return pieces
        full_tag = mo.group(1)
        if not full_tag.startswith(tag_prefix):
            if verbose:
                fmt = "tag '%s' doesn't start with prefix '%s'"
                print(fmt % (full_tag, tag_prefix))
            pieces["error"] = "tag '{}' doesn't start with prefix '{}'".format(
                full_tag, tag_prefix
            )
            return pieces
        pieces["closest-tag"] = full_tag[len(tag_prefix) :]
        pieces["distance"] = int(mo.group(2))
        pieces["short"] = mo.group(3)
    else:
        pieces["closest-tag"] = None
        (count_out, rc) = run_command(GITS, ["rev-list", "HEAD", "--count"], cwd=root)
        pieces["distance"] = int(count_out)
    date = run_command(GITS, ["show", "-s", "--format=%ci", "HEAD"], cwd=root)[
        0
    ].strip()
    pieces["date"] = date.strip().replace(" ", "T", 1).replace(" ", "", 1)
    return pieces


def plus_or_dot(pieces):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Return a + if we don't already have one, else return a ."
    if "+" in pieces.get("closest-tag", ""):
        return "."
    return "+"


def render_pep440(pieces):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    'Build up version string, with post-release "local version identifier".\n\n    Our goal: TAG[+DISTANCE.gHEX[.dirty]] . Note that if you\n    get a tagged build and then dirty it, you\'ll get TAG+0.gHEX.dirty\n\n    Exceptions:\n    1: no tags. git_describe was just HEX. 0+untagged.DISTANCE.gHEX[.dirty]\n    '
    if pieces["closest-tag"]:
        rendered = pieces["closest-tag"]
        if pieces["distance"] or pieces["dirty"]:
            rendered += plus_or_dot(pieces)
            rendered += "%d.g%s" % (pieces["distance"], pieces["short"])
            if pieces["dirty"]:
                rendered += ".dirty"
    else:
        rendered = "0+untagged.%d.g%s" % (pieces["distance"], pieces["short"])
        if pieces["dirty"]:
            rendered += ".dirty"
    return rendered


def render_pep440_pre(pieces):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "TAG[.post.devDISTANCE] -- No -dirty.\n\n    Exceptions:\n    1: no tags. 0.post.devDISTANCE\n    "
    if pieces["closest-tag"]:
        rendered = pieces["closest-tag"]
        if pieces["distance"]:
            rendered += ".post.dev%d" % pieces["distance"]
    else:
        rendered = "0.post.dev%d" % pieces["distance"]
    return rendered


def render_pep440_post(pieces):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    'TAG[.postDISTANCE[.dev0]+gHEX] .\n\n    The ".dev0" means dirty. Note that .dev0 sorts backwards\n    (a dirty tree will appear "older" than the corresponding clean one),\n    but you shouldn\'t be releasing software with -dirty anyways.\n\n    Exceptions:\n    1: no tags. 0.postDISTANCE[.dev0]\n    '
    if pieces["closest-tag"]:
        rendered = pieces["closest-tag"]
        if pieces["distance"] or pieces["dirty"]:
            rendered += ".post%d" % pieces["distance"]
            if pieces["dirty"]:
                rendered += ".dev0"
            rendered += plus_or_dot(pieces)
            rendered += f"g{pieces['short']}"
    else:
        rendered = "0.post%d" % pieces["distance"]
        if pieces["dirty"]:
            rendered += ".dev0"
        rendered += f"+g{pieces['short']}"
    return rendered


def render_pep440_old(pieces):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    'TAG[.postDISTANCE[.dev0]] .\n\n    The ".dev0" means dirty.\n\n    Eexceptions:\n    1: no tags. 0.postDISTANCE[.dev0]\n    '
    if pieces["closest-tag"]:
        rendered = pieces["closest-tag"]
        if pieces["distance"] or pieces["dirty"]:
            rendered += ".post%d" % pieces["distance"]
            if pieces["dirty"]:
                rendered += ".dev0"
    else:
        rendered = "0.post%d" % pieces["distance"]
        if pieces["dirty"]:
            rendered += ".dev0"
    return rendered


def render_git_describe(pieces):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "TAG[-DISTANCE-gHEX][-dirty].\n\n    Like 'git describe --tags --dirty --always'.\n\n    Exceptions:\n    1: no tags. HEX[-dirty]  (note: no 'g' prefix)\n    "
    if pieces["closest-tag"]:
        rendered = pieces["closest-tag"]
        if pieces["distance"]:
            rendered += "-%d-g%s" % (pieces["distance"], pieces["short"])
    else:
        rendered = pieces["short"]
    if pieces["dirty"]:
        rendered += "-dirty"
    return rendered


def render_git_describe_long(pieces):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "TAG-DISTANCE-gHEX[-dirty].\n\n    Like 'git describe --tags --dirty --always -long'.\n    The distance/hash is unconditional.\n\n    Exceptions:\n    1: no tags. HEX[-dirty]  (note: no 'g' prefix)\n    "
    if pieces["closest-tag"]:
        rendered = pieces["closest-tag"]
        rendered += "-%d-g%s" % (pieces["distance"], pieces["short"])
    else:
        rendered = pieces["short"]
    if pieces["dirty"]:
        rendered += "-dirty"
    return rendered


def render(pieces, style):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Render the given version pieces into the requested style."
    if pieces["error"]:
        return {
            "version": "unknown",
            "full-revisionid": pieces.get("long"),
            "dirty": None,
            "error": pieces["error"],
            "date": None,
        }
    if (not style) or (style == "default"):
        style = "pep440"
    if style == "pep440":
        rendered = render_pep440(pieces)
    elif style == "pep440-pre":
        rendered = render_pep440_pre(pieces)
    elif style == "pep440-post":
        rendered = render_pep440_post(pieces)
    elif style == "pep440-old":
        rendered = render_pep440_old(pieces)
    elif style == "git-describe":
        rendered = render_git_describe(pieces)
    elif style == "git-describe-long":
        rendered = render_git_describe_long(pieces)
    else:
        raise ValueError(f"unknown style '{style}'")
    return {
        "version": rendered,
        "full-revisionid": pieces["long"],
        "dirty": pieces["dirty"],
        "error": None,
        "date": pieces.get("date"),
    }


def get_versions():
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Get version information or return default if unable to do so."
    cfg = get_config()
    verbose = cfg.verbose
    try:
        return git_versions_from_keywords(get_keywords(), cfg.tag_prefix, verbose)
    except NotThisMethod:
        pass
    try:
        root = os.path.realpath(__file__)
        for _ in cfg.versionfile_source.split("/"):
            root = os.path.dirname(root)
    except NameError:
        return {
            "version": "0+unknown",
            "full-revisionid": None,
            "dirty": None,
            "error": "unable to find root of source tree",
            "date": None,
        }
    try:
        pieces = git_pieces_from_vcs(cfg.tag_prefix, root, verbose)
        return render(pieces, cfg.style)
    except NotThisMethod:
        pass
    try:
        if cfg.parentdir_prefix:
            return versions_from_parentdir(cfg.parentdir_prefix, root, verbose)
    except NotThisMethod:
        pass
    return {
        "version": "0+unknown",
        "full-revisionid": None,
        "dirty": None,
        "error": "unable to compute version",
        "date": None,
    }
