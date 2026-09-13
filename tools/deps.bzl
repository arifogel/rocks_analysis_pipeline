"Utility macros"

load("@rules_python//python:defs.bzl", canonical_py_library = "py_library")

def cdeps(prod, *, dev = []):
    """Concatenates production dependencies with conditional dev dependencies.

    Args:
        prod: List of labels for production dependencies (positional).
        dev:  List of labels for development dependencies (keyword-only).
    """
    return prod + select({
        "@pypi//venv:dev": dev,
        "//conditions:default": [],
    })

def python_ide_backport(name = "cresproc_ide"):
    canonical_py_library(
        name = name,
        srcs = native.glob(["**/*.py"]),
        visibility = ["//visibility:private"],
    )
