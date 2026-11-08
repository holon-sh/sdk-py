"""Stand-in for the client ``holon types generate`` writes into the project.

``holon`` is a namespace package, so the project's generated ``holon/apps`` is
found here even when the project comes after site-packages on ``sys.path``.
"""

import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import holon

_here = Path(__file__).resolve().parent
for _portion in holon.__path__:
    _generated = Path(_portion, "apps")
    if _generated.resolve() != _here and (_generated / "__init__.py").is_file():
        _spec = spec_from_file_location(
            __name__,
            _generated / "__init__.py",
            submodule_search_locations=[str(_generated)],
        )
        assert _spec and _spec.loader
        sys.modules[__name__] = _module = module_from_spec(_spec)
        _spec.loader.exec_module(_module)
        break
else:
    raise ImportError(
        "no generated holon.apps on sys.path: run `holon types generate` in the project"
    )
