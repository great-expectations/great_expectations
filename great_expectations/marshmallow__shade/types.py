
'Type aliases.\n\n.. warning::\n\n    This module is provisional. Types may be modified, added, and removed between minor releases.\n'
import typing
StrSequenceOrSet = typing.Union[(typing.Sequence[str], typing.Set[str])]
Tag = typing.Union[(str, typing.Tuple[(str, bool)])]
