from __future__ import annotations

import ast
import hashlib
import os
import re
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Never

IGNORED = frozenset(
    {
        ".git",
        ".venv",
        "venv",
        "node_modules",
        "holon",
        ".holon",
        ".holon-types",
        "__pycache__",
    }
)
ALIASES = {
    "typing_extensions": "typing",
    "mcp.server.mcpserver": "mcp.server",
    "mcp.server.lowlevel": "mcp.server",
}
SERVERS = {"mcp.server:MCPServer", "mcp.server:Server"}
REGISTRATIONS = {"tool", "add_tool", "list_tools", "remove_tool"}
CONSTRUCTOR_CATALOGS = {"tools", "on_list_tools", None}
FUNCTIONS = (ast.FunctionDef, ast.AsyncFunctionDef)
ASSIGNMENTS = (ast.Assign, ast.AnnAssign, ast.AugAssign)
SCALARS: dict[object, dict[str, Any]] = {
    "builtins:str": {"type": "string"},
    "builtins:int": {"type": "integer"},
    "builtins:float": {"type": "number"},
    "builtins:bool": {"type": "boolean"},
    "builtins:None": {"type": "null"},
    "typing:Any": {},
    "builtins:object": {},
}
LITERALS = {str: "string", int: "integer", bool: "boolean", type(None): "null"}
FIELDS = {
    "default": "default",
    "gt": "exclusiveMinimum",
    "ge": "minimum",
    "lt": "exclusiveMaximum",
    "le": "maximum",
    "multiple_of": "multipleOf",
    "pattern": "pattern",
    "description": "description",
    "title": "title",
}
OPTIONS = {
    "name",
    "title",
    "description",
    "annotations",
    "icons",
    "meta",
    "structured_output",
}
MODIFIERS: dict[object, bool] = {"typing:NotRequired": True, "typing:Required": False}
CATALOG = {
    "name": "name",
    "input_schema": "inputSchema",
    "output_schema": "outputSchema",
}
DUPLICATE = "duplicate"


class ExtractionError(ValueError):
    pass


def uses_generated_imports(source: str) -> bool:
    """Whether a source imports the application's generated namespace portion."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return bool(
            re.search(
                r"(?m)^\s*(?:from\s+holon\.apps\b|import[^\n]*\bholon\.apps\b)",
                source,
            )
        )
    return any(
        isinstance(node, ast.ImportFrom)
        and node.level == 0
        and node.module == "holon.apps"
        or isinstance(node, ast.Import)
        and any(alias.name == "holon.apps" for alias in node.names)
        for node in ast.walk(tree)
    )


class Analyzer:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.trees: dict[Path, ast.Module] = {}
        self.modules: dict[Path, dict[str, ast.AST | str]] = {}
        self.active: set[tuple[Path, str]] = set()
        self.sources: dict[str, str] = {}
        self.definitions: dict[str, dict[str, Any]] = {}

    def fail(self, path: Path, node: ast.AST, message: str) -> Never:
        raise ExtractionError(
            f"{path.relative_to(self.root)}:{getattr(node, 'lineno', 1)}:{getattr(node, 'col_offset', 0) + 1}: {message}"
        )

    @contextmanager
    def visiting(self, path: Path, node: ast.AST, name: str) -> Iterator[None]:
        if (path, name) in self.active:
            self.fail(path, node, "Recursive or cyclic contracts are unsupported.")
        self.active.add((path, name))
        try:
            yield
        finally:
            self.active.remove((path, name))

    def tree(self, path: Path) -> ast.Module:
        path = path.resolve()
        if path not in self.trees:
            self.trees[path] = ast.parse(
                path.read_text(), filename=str(path.relative_to(self.root))
            )
        return self.trees[path]

    def load(self, path: Path) -> dict[str, ast.AST | str]:
        if path not in self.modules:
            symbols = self.modules[path] = {}
            for node in self.tree(path).body:
                for name, value in self.bindings(path, node):
                    symbols[name] = DUPLICATE if name in symbols else value
        return self.modules[path]

    def bindings(self, path: Path, node: ast.stmt) -> list[tuple[str, ast.AST | str]]:
        match node:
            case ast.ImportFrom(module=module, level=level, names=names):
                prefix = "." * level + (module or "")
                return [
                    (alias.asname or alias.name, f"{prefix}:{alias.name}")
                    for alias in names
                ]
            case ast.Import(names=names):
                return [
                    (
                        name := alias.asname or alias.name.split(".")[0],
                        f"{alias.name if alias.asname else name}:",
                    )
                    for alias in names
                ]
            case (
                ast.ClassDef(name=name)
                | ast.FunctionDef(name=name)
                | ast.AsyncFunctionDef(name=name)
            ):
                return [(name, node)]
            case ast.Assign() | ast.AnnAssign() | ast.AugAssign():
                targets = (
                    node.targets if isinstance(node, ast.Assign) else [node.target]
                )
                if any(
                    isinstance(target, (ast.Attribute, ast.Subscript))
                    for target in targets
                ):
                    self.fail(
                        path,
                        node,
                        "Module-level schema or catalog mutation is unsupported.",
                    )
                value = node if isinstance(node, ast.AugAssign) else node.value
                if value is None:
                    return []
                return [
                    (target.id, value)
                    for target in targets
                    if isinstance(target, ast.Name)
                ]
        return []

    def resolve(self, path: Path, node: ast.AST) -> tuple[Path, ast.AST | str]:
        match node:
            case ast.Constant(value=None):
                return path, "builtins:None"
            case ast.Constant(value=str() as text):
                return self.resolve(path, ast.parse(text, mode="eval").body)
            case ast.Attribute(value=base, attr=attr):
                source, value = self.resolve(path, base)
                if isinstance(value, str):
                    return self.imported(
                        source, value.replace(":", ".").rstrip(".") + ":" + attr
                    )
                return source, node
            case ast.Name(id=name):
                value = self.load(path).get(name, "builtins:" + name)
                if value == DUPLICATE:
                    self.fail(
                        path,
                        node,
                        "Reassigned contract symbols cannot be established statically.",
                    )
                with self.visiting(path, node, name):
                    if isinstance(value, str):
                        return self.imported(path, value)
                    if isinstance(value, (ast.Name, ast.Attribute)):
                        return self.resolve(path, value)
                    return path, value
        return path, node

    def imported(self, path: Path, symbol: str) -> tuple[Path, ast.AST | str]:
        module, _, name = symbol.rpartition(":")
        level = len(module) - len(module.lstrip("."))
        base = path.parent.joinpath(*[".."] * (level - 1)) if level else self.root
        parts = module.lstrip(".").split(".")
        if not level and parts[0] in IGNORED:
            return path, f"{module}:{name}"
        local = base.joinpath(*parts).with_suffix(".py")
        if not local.is_file():
            local = base.joinpath(*parts, "__init__.py")
        local = local.resolve()
        submodule = base.joinpath(*parts, name).with_suffix(".py").resolve()
        if name and submodule.is_file() and submodule.is_relative_to(self.root):
            return path, f"{module}.{name}:"
        if name and local.is_file() and local.is_relative_to(self.root):
            return self.resolve(local, ast.Name(id=name))
        return path, f"{ALIASES.get(module, module)}:{name}"

    def generic(
        self, path: Path, node: ast.AST
    ) -> tuple[Path, ast.AST | str, list[ast.expr]]:
        source, value = self.resolve(path, node)
        if isinstance(value, ast.Subscript):
            args = (
                value.slice.elts
                if isinstance(value.slice, ast.Tuple)
                else [value.slice]
            )
            return source, self.resolve(source, value.value)[1], args
        return source, value, []

    def arms(self, path: Path, node: ast.AST) -> list[tuple[Path, ast.AST]]:
        source, origin, args = self.generic(path, node)
        if isinstance(origin, ast.BinOp) and isinstance(origin.op, ast.BitOr):
            return self.arms(source, origin.left) + self.arms(source, origin.right)
        if origin == "typing:Optional":
            return self.arms(source, args[0]) + [(source, ast.Constant(None))]
        if origin == "typing:Union":
            return [arm for arg in args for arm in self.arms(source, arg)]
        return [(path, node)]

    def is_server(self, path: Path, node: ast.AST) -> bool:
        source, value = self.resolve(path, node)
        return (
            isinstance(value, ast.Call)
            and self.resolve(source, value.func)[1] in SERVERS
        )

    def constant(self, path: Path, node: ast.AST) -> Any:
        if isinstance(node, ast.Constant):
            return node.value
        _, target = self.resolve(path, node)
        return target.value if isinstance(target, ast.Constant) else None

    def literal(self, path: Path, node: ast.AST) -> Any:
        match node:
            case ast.Constant(value=constant):
                return constant
            case ast.UnaryOp(op=ast.USub(), operand=operand) if isinstance(
                number := self.literal(path, operand), (int, float)
            ):
                return -number
            case ast.List(elts=items) | ast.Tuple(elts=items):
                return [self.literal(path, item) for item in items]
            case ast.Dict(keys=keys, values=values):
                result: dict[Any, Any] = {}
                for key, item in zip(keys, values):
                    result.update(
                        self.literal(path, item)
                        if key is None
                        else {self.literal(path, key): self.literal(path, item)}
                    )
                return result
        source, target = self.resolve(path, node)
        if isinstance(target, ast.AST) and target is not node:
            return self.literal(source, target)
        return self.fail(
            path,
            node,
            "Expected a literal contract value; application code is never evaluated.",
        )

    def schema(self, path: Path, node: ast.AST) -> dict[str, Any]:
        members = self.arms(path, node)
        if len(members) > 1:
            return {"anyOf": [self.schema(*member) for member in members]}
        path, node = members[0]
        source, origin, args = self.generic(path, node)
        if isinstance(origin, ast.ClassDef):
            model = self.model(source, origin)
            if self.definitions.setdefault(origin.name, model) != model:
                self.fail(source, origin, "Conflicting model names in one tool schema.")
            return {"$ref": f"#/$defs/{origin.name}"}
        if origin in SCALARS:
            return dict(SCALARS[origin])
        if origin in GENERICS and args:
            return GENERICS[origin](self, source, args)
        return self.fail(path, node, "Schema cannot be established statically.")

    def annotated(self, path: Path, args: list[ast.expr]) -> dict[str, Any]:
        result = self.schema(path, args[0])
        for extra in args[1:]:
            result.update(self.field(path, extra))
        return result

    def required(self, path: Path, args: list[ast.expr]) -> dict[str, Any]:
        return self.schema(path, args[0])

    def enumeration(self, path: Path, args: list[ast.expr]) -> dict[str, Any]:
        values = [self.literal(path, item) for item in args]
        result: dict[str, Any] = (
            {"const": values[0]} if len(values) == 1 else {"enum": values}
        )
        kinds = {LITERALS.get(type(value)) for value in values}
        if len(kinds) == 1 and None not in kinds:
            result["type"] = kinds.pop()
        return result

    def array(self, path: Path, args: list[ast.expr]) -> dict[str, Any]:
        return {"type": "array", "items": self.schema(path, args[0])}

    def sequence(self, path: Path, args: list[ast.expr]) -> dict[str, Any]:
        match args:
            case [item, ast.Constant(value=builtin)] if builtin is Ellipsis:
                return self.array(path, [item])
        items = [self.schema(path, item) for item in args]
        return {
            "type": "array",
            "prefixItems": items,
            "minItems": len(items),
            "maxItems": len(items),
        }

    def mapping(self, path: Path, args: list[ast.expr]) -> dict[str, Any]:
        if self.schema(path, args[0]) != {"type": "string"}:
            self.fail(path, args[0], "Wire dictionary keys must be strings.")
        return {"type": "object", "additionalProperties": self.schema(path, args[1])}

    def model(self, path: Path, value: ast.ClassDef) -> dict[str, Any]:
        bases = [self.resolve(path, base) for base in value.bases]
        if value.decorator_list or len(bases) != 1:
            self.fail(
                path, value, "Use an undecorated BaseModel or TypedDict contract."
            )
        source, base = bases[0]
        if isinstance(base, ast.ClassDef):
            result = self.model(source, base)
        elif base in ("pydantic:BaseModel", "typing:TypedDict"):
            result = {"type": "object", "properties": {}}
        else:
            self.fail(
                path, value, "Use an undecorated BaseModel or TypedDict contract."
            )
        total = all(
            self.literal(path, item.value)
            for item in value.keywords
            if item.arg == "total"
        )
        required = result.pop("required", [])
        with self.visiting(path, value, value.name):
            for member in value.body:
                match member:
                    case ast.AnnAssign(
                        target=ast.Name(id=name), annotation=annotation
                    ) if not name.startswith("_"):
                        field = self.declared(path, annotation, member.value)
                        origin = self.generic(path, annotation)[1]
                        optional = MODIFIERS.get(origin, not total)
                        if "default" not in field and not optional:
                            required.append(name)
                        result["properties"][name] = field
                    case ast.Assign(targets=[ast.Name(id="model_config")]):
                        config = self.literal(path, member.value)
                        if set(config) - {"extra"}:
                            self.fail(path, member, "Unsupported model configuration.")
                        if config.get("extra") in {"forbid", "allow"}:
                            result["additionalProperties"] = config["extra"] == "allow"
                    case ast.Pass() | ast.Expr(value=ast.Constant()):
                        pass
                    case _:
                        self.fail(
                            path,
                            member,
                            "Private fields, methods, validators and computed fields are unsupported.",
                        )
        if required:
            result["required"] = required
        return result

    def field(self, path: Path, node: ast.AST) -> dict[str, Any]:
        if (
            not isinstance(node, ast.Call)
            or self.resolve(path, node.func)[1] != "pydantic:Field"
            or len(node.args) > 1
        ):
            self.fail(path, node, "Only static pydantic.Field metadata is supported.")
        result = {}
        for name, value in [
            *(("default", item) for item in node.args),
            *((item.arg, item.value) for item in node.keywords),
        ]:
            if name not in FIELDS:
                self.fail(
                    path,
                    value,
                    f"Unsupported Field option {name}; no callbacks are evaluated.",
                )
            if (literal := self.literal(path, value)) is not Ellipsis:
                result[FIELDS[name]] = literal
        return result

    def declared(
        self, path: Path, annotation: ast.expr, default: ast.expr | None
    ) -> dict[str, Any]:
        schema = self.schema(path, annotation)
        if isinstance(default, ast.Call):
            schema.update(self.field(path, default))
        elif default is not None:
            schema["default"] = self.literal(path, default)
        return schema

    def tool(
        self,
        path: Path,
        function: ast.FunctionDef | ast.AsyncFunctionDef,
        options: Mapping[str, ast.AST],
    ) -> dict[str, Any]:
        name = self.literal(path, options.get("name", ast.Constant(function.name)))
        if not isinstance(name, str) or not name:
            self.fail(path, function, "Tool names must be literal nonempty strings.")
        signature = function.args
        if signature.posonlyargs or signature.vararg or signature.kwarg:
            self.fail(
                path,
                function,
                "Variadic and positional-only tool parameters are unsupported.",
            )
        self.definitions = {}
        properties = {}
        required = []
        defaults = (
            [None] * (len(signature.args) - len(signature.defaults))
            + signature.defaults
            + signature.kw_defaults
        )
        for arg, default in zip(signature.args + signature.kwonlyargs, defaults):
            if arg.annotation is None:
                self.fail(
                    path, arg, "Typed tool parameters require explicit annotations."
                )
            if self.generic(path, arg.annotation)[1] == "mcp.server:Context":
                continue
            schema = self.declared(path, arg.annotation, default)
            if "default" not in schema:
                required.append(arg.arg)
            properties[arg.arg] = schema
        result: dict[str, Any] = {
            "name": name,
            "inputSchema": {"type": "object", "properties": properties},
        }
        if required:
            result["inputSchema"]["required"] = required
        result["inputSchema"] = self.with_definitions(result["inputSchema"])
        structured = options.get("structured_output", ast.Constant(None))
        if (
            function.returns
            and self.literal(path, structured) is not False
            and (output := self.output(path, function.returns))
        ):
            result["outputSchema"] = self.with_definitions(output)
        return result

    def with_definitions(self, schema: dict[str, Any]) -> dict[str, Any]:
        definitions, self.definitions = self.definitions, {}
        if "$ref" in schema:
            schema = definitions.pop(schema["$ref"].removeprefix("#/$defs/"))
        if definitions:
            schema["$defs"] = definitions
        return schema

    def output(self, path: Path, returns: ast.expr) -> dict[str, Any] | None:
        parts = [
            arm
            for arm in self.arms(path, returns)
            if self.generic(*arm)[1] != "mcp.types:InputRequiredResult"
        ]
        if not parts:
            return None
        if len(parts) > 1:
            return wrap({"anyOf": [self.schema(*part) for part in parts]})
        source, node = parts[0]
        _, origin, args = self.generic(source, node)
        if (
            origin == "typing:Annotated"
            and self.generic(source, args[0])[1] == "mcp.types:CallToolResult"
        ):
            if len(args) != 2:
                self.fail(
                    source,
                    node,
                    "Use one explicit output model with Annotated[CallToolResult, Model].",
                )
            node = args[1]
            origin = self.generic(source, node)[1]
        if origin in {"mcp.types:CallToolResult", "typing:Any"}:
            return None
        schema = self.schema(source, node)
        return (
            schema
            if isinstance(origin, ast.ClassDef) or origin == "builtins:dict"
            else wrap(schema)
        )

    def registration(self, node: ast.AST, servers: set[str]) -> str | None:
        match node:
            case ast.Call(func=ast.Attribute(value=ast.Name(id=name), attr=attr)) if (
                name in servers
            ):
                return attr
        return None

    def decorated(
        self,
        path: Path,
        function: ast.FunctionDef | ast.AsyncFunctionDef,
        call: ast.Call,
    ) -> list[dict[str, Any]]:
        options: dict[str | None, ast.AST] = {
            item.arg: item.value for item in call.keywords
        }
        if None in options or len(call.args) > 1 or set(options) - OPTIONS:
            self.fail(path, call, "Computed registration options are unsupported.")
        if call.args:
            options["name"] = call.args[0]
        return [
            self.tool(
                path, function, {str(name): value for name, value in options.items()}
            )
        ]

    def resource(
        self,
        path: Path,
        function: ast.FunctionDef | ast.AsyncFunctionDef,
        call: ast.Call,
    ) -> list[dict[str, Any]]:
        options: dict[str | None, ast.AST] = {
            item.arg: item.value for item in call.keywords
        }
        if call.args:
            options["uri"] = call.args[0]
        mime_type = self.constant(path, options.get("mime_type", ast.Constant(None)))
        if mime_type != "application/json" or function.returns is None:
            return []
        uri = self.constant(path, options.get("uri", ast.Constant(None)))
        if not isinstance(uri, str) or not uri:
            self.fail(path, call, "Typed resources require a constant URI.")
        return [
            {
                "uri": uri,
                "outputSchema": self.with_definitions(
                    self.schema(path, function.returns)
                ),
            }
        ]

    def catalog(
        self,
        path: Path,
        function: ast.FunctionDef | ast.AsyncFunctionDef,
        call: ast.Call,
    ) -> list[dict[str, Any]]:
        body = (
            function.body[1:]
            if ast.get_docstring(function) is not None
            else function.body
        )
        match body:
            case [ast.Return(value=ast.List(elts=items) | ast.Tuple(elts=items))]:
                return [self.listed(path, item) for item in items]
        return self.fail(
            path, function, "tools/list must return a fixed literal catalog."
        )

    def listed(self, path: Path, item: ast.expr) -> dict[str, Any]:
        match item:
            case ast.Call(func=func, args=[], keywords=keywords) if (
                self.resolve(path, func)[1] == "mcp.types:Tool"
            ):
                fields = {
                    keyword.arg: self.literal(path, keyword.value)
                    for keyword in keywords
                }
                tool = {
                    CATALOG[name]: value
                    for name, value in fields.items()
                    if name in CATALOG
                }
                if None not in fields and {"name", "inputSchema"} <= set(tool):
                    return tool
        return self.fail(
            path,
            item,
            "Expected a literal official MCP Tool with name and input_schema.",
        )

    def added(self, path: Path, call: ast.Call) -> list[dict[str, Any]]:
        match call:
            case ast.Call(args=[argument], keywords=[]):
                source, value = self.resolve(path, argument)
                if isinstance(value, FUNCTIONS) and not value.decorator_list:
                    return [self.tool(source, value, {})]
        return self.fail(
            path,
            call,
            "add_tool requires one statically resolvable undecorated function.",
        )

    def extract(self, path: Path) -> dict[str, list[dict[str, Any]]]:
        tree = self.tree(path)
        servers = {
            name for name in self.load(path) if self.is_server(path, ast.Name(id=name))
        }
        if (
            "server" not in servers
            or len({self.resolve(path, ast.Name(id=name))[1] for name in servers}) != 1
        ):
            self.fail(
                path,
                tree,
                "The entrypoint must expose one official MCP server as server.",
            )
        match self.resolve(path, ast.Name(id="server")):
            case (source, ast.Call(keywords=keywords) as construction) if any(
                item.arg in CONSTRUCTOR_CATALOGS for item in keywords
            ):
                self.fail(
                    source,
                    construction,
                    "Register tools only with top-level MCP decorators or add_tool in the entrypoint.",
                )
        tools: list[dict[str, Any]] = []
        resources: list[dict[str, Any]] = []
        handled = set()
        for node in tree.body:
            match node:
                case (
                    ast.FunctionDef(decorator_list=[ast.Call() as call])
                    | ast.AsyncFunctionDef(decorator_list=[ast.Call() as call])
                ) if (kind := self.registration(call, servers)) in DECORATORS:
                    handled.add(call.func)
                    registered = resources if kind == "resource" else tools
                    registered += DECORATORS[kind](self, path, node, call)
                case ast.Expr(value=ast.Call() as call) if (
                    self.registration(call, servers) == "add_tool"
                ):
                    handled.add(call.func)
                    tools += self.added(path, call)
                case ast.Expr(value=ast.Call(func=func)) if isinstance(
                    self.resolve(path, func)[1], FUNCTIONS
                ):
                    self.fail(
                        path,
                        node,
                        "Module-level helper execution cannot establish a static catalog.",
                    )
                case ast.If() | ast.For() | ast.While() | ast.With() | ast.Try() if any(
                    isinstance(item, ASSIGNMENTS) for item in ast.walk(node)
                ):
                    self.fail(
                        path,
                        node,
                        "Conditional or computed module assignments are unsupported.",
                    )
        for child in ast.walk(tree):
            match child:
                case ast.Call(args=args, keywords=keywords) if any(
                    isinstance(item, ast.Name) and item.id in servers
                    for item in [*args, *(keyword.value for keyword in keywords)]
                ):
                    self.fail(
                        path,
                        child,
                        "Passing the server to other code cannot establish a static contract.",
                    )
        for source, module in list(self.trees.items()):
            for child in ast.walk(module):
                if (
                    isinstance(child, ast.Attribute)
                    and child.attr in REGISTRATIONS
                    and child not in handled
                    and self.is_server(source, child.value)
                ):
                    self.fail(
                        source,
                        child,
                        "Register tools only with top-level MCP decorators or add_tool in the entrypoint.",
                    )
        names = [tool["name"] for tool in tools]
        uris = [resource["uri"] for resource in resources]
        if len(names) != len(set(names)) or len(uris) != len(set(uris)):
            self.fail(path, tree, "Duplicate tool registrations are unsupported.")
        return {"tools": tools, "resources": resources}


def wrap(schema: dict[str, Any]) -> dict[str, Any]:
    return {"type": "object", "properties": {"result": schema}, "required": ["result"]}


def extract(root: str | Path, entry: str) -> dict[str, Any]:
    analyzer = Analyzer(Path(root))
    uses_apps = False
    try:
        paths = []
        for directory, folders, filenames in os.walk(analyzer.root):
            folders[:] = [name for name in folders if name not in IGNORED]
            paths += [
                Path(directory, name) for name in filenames if name.endswith(".py")
            ]
        for path in paths:
            source = path.read_text()
            analyzer.sources[str(path.relative_to(analyzer.root))] = hashlib.sha256(
                source.encode()
            ).hexdigest()
            uses_apps |= uses_generated_imports(source)
        for path in paths:
            analyzer.tree(path)
        catalog = analyzer.extract((analyzer.root / entry).resolve())
        return {**catalog, "usesApps": uses_apps, "sources": analyzer.sources}
    except (
        ValueError,
        SyntaxError,
        OSError,
        LookupError,
        TypeError,
        RecursionError,
    ) as error:
        return {
            "tools": [],
            "diagnostic": str(error),
            "usesApps": uses_apps,
            "sources": analyzer.sources,
        }


DECORATORS = {
    "tool": Analyzer.decorated,
    "list_tools": Analyzer.catalog,
    "resource": Analyzer.resource,
}
GENERICS: dict[object, Callable[[Analyzer, Path, list[ast.expr]], dict[str, Any]]] = {
    "typing:Annotated": Analyzer.annotated,
    "typing:NotRequired": Analyzer.required,
    "typing:Required": Analyzer.required,
    "typing:Literal": Analyzer.enumeration,
    "builtins:list": Analyzer.array,
    "typing:List": Analyzer.array,
    "builtins:tuple": Analyzer.sequence,
    "typing:Tuple": Analyzer.sequence,
    "builtins:dict": Analyzer.mapping,
    "typing:Dict": Analyzer.mapping,
}
