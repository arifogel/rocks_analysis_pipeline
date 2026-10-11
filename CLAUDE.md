# Comment/docstring/prose style (this repo)

Applies to every comment, docstring, and prose doc in this repo (.bzl/BUILD comments, NOTES.md,
PR descriptions, etc.), except where a rule says otherwise. README.md is exempt from the
Bazel fluency assumption below (rule 4) but not from the others.

1. State current behavior only. Never PR/change history ("was fixed", "confirmed via",
   "originally", "used to"). Never negative framing ("not X", "isn't Y", "no longer") where a
   positive statement works. Never decorative contrast ("unlike Y", "as opposed to") unless the
   contrast IS the non-obvious fact being conveyed. Never justify the obvious. Never defend
   against an objection nobody raised ("to be clear, this is intentional").

2. A file's docs may reference what it depends on: something it wraps, extends, is built from,
   or otherwise reaches downward into. Never reference what depends on it (a caller) or what
   plays an equivalent role elsewhere (a sibling) — those go stale and make this file's meaning
   contingent on another file's. If a fact seems to need pointing at a caller/sibling, restate
   the fact here, or drop it.

3. Be terse. Prefer one sentence over three. Cut a sentence if removing it loses no information
   a future reader needs. Never break a long sentence's flow with a large parenthetical - end
   the sentence and start a new one, or drop the aside.

4. In a Bazel file or Bazel-centric doc (NOTES.md included), assume the reader knows Bazel and
   Python build tooling. Don't explain what a rule/macro/repository_ctx/py_binary/etc. is, or
   restate what code obviously does. Comment only non-obvious behavior, a real gotcha, or the
   reason for a choice that isn't the first one you'd guess. (README.md may need to explain more,
   since it may reach readers without that background.)

5. A docstring states purpose and interface - what a function does, its args, its return - not
   internal mechanism. Mechanism isn't a contract and shouldn't read as one. Every function
   docstring needs a one-line summary, then Args:, then Returns: if it returns a value -
   buildifier enforces this; match its exact format and indentation.

Before writing or editing a comment: does this state a fact about right now, standalone, tersely,
assuming the reader's fluency? If not, cut or rewrite.

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
