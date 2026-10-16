# Comment/docstring/prose style (this repo)

Applies to every comment, docstring, and prose doc in this repo (.bzl/BUILD comments,
NOTES.md, PR descriptions, etc.). README.md may explain more, since it can reach readers
without the fluency rule 5 assumes.

## 1. Decide whether to write it

For each explanatory sentence: would a competent reader, with ordinary background for this
domain, reach a wrong conclusion or stay confused without it? If no one would have gotten
it wrong, omit it, however true it is.

Write it when it:
- corrects a plausible false expectation;
- gives domain or empirical reasoning tied to an actual finding;
- says where a valid value comes from, when a required field has no sensible default;
- explains a contestable methodological or architectural tradeoff.

Omit:
- what the code, language, or standard library already shows (e.g. noting that
  `dataclasses.replace()` returns a new object, directly above a call to `replace()`);
- justification of the obvious or of standard practice;
- defenses against objections nobody raised;
- preambles announcing background or audience ("Background, for readers unfamiliar with...").

## 2. Shape what you write

1. State current behavior only. No change or PR history ("was fixed", "originally",
   "used to", "no longer"). Give the present design and its present rationale.
2. Prefer positive statements to negative ones ("not X", "isn't Y"). Use contrast
   ("unlike Y", "as opposed to") only when the contrast is the non-obvious fact.
3. Be terse. Prefer one sentence over three, and cut a sentence that loses nothing. End a
   long sentence and start a new one rather than adding a large parenthetical.
4. Reference only what this file depends on: something it wraps, extends, is built from,
   or reaches into. Never reference callers or siblings; restate the fact here or drop it.
5. Assume a reader fluent in Bazel and Python build tooling. Comment only non-obvious
   behavior, a real gotcha, or the reason for a choice that isn't the first one you'd guess.

## 3. Docstrings

State purpose and interface (what it does, its args, its return), not internal mechanism.
Every function docstring has a one-line summary, then `Args:`, then `Returns:` when it
returns a value. Starlark docstrings match buildifier's exact format and indentation.

## 4. Before finishing

Re-read every comment, docstring, and PR description you added. Re-check against section 1
any sentence containing: "used to", "no longer", "originally", "was fixed", "previously",
"unlike", "not X but", "to be clear", "Background", "for readers unfamiliar". Confirm each
doc names only dependencies and each docstring has the format above.

Example. Change history becomes current behavior:
- Before: "Returns the user list. Previously a dict; now a list."
- After: "Returns the user list."

Example. Defending the obvious becomes a description:
- Before: `timeout: float  """Required: a missing timeout would be a bug."""`
- After: `timeout: float  """Seconds to wait for the server."""`

# Python code design

1. Closed sets of names are enums. Strings appear only at boundaries (CLI, filenames,
   serialization) and are converted on the way in and out.
2. Dataclasses are `frozen=True, kw_only=True`. Enum members that carry data wrap a
   kwargs-constructed dataclass, never a positional tuple.
3. Functions don't return tuples. Return a named type, or restructure so the caller gets one
   thing. Exceptions: idiomatic compatibility with external code, or a specific performance
   justification.
4. Read-only parameters use `collections.abc` types (`Mapping`, `Set`, `Sequence`, `Callable`).
   Concrete `dict`, `list` and `set` only where the function mutates them.
5. Define each concept once. When adding a name, flag or constant, search for other places
   that list it, and derive them all from one definition (an enum, a constant or a shared
   helper).
