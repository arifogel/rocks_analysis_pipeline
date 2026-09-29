"""warm_py_binary(): runs a py_binary once at build time to force its lazily-created venv into
existence, then captures its whole runfiles tree - interpreter, packages, warmed venv, and the
binary's own data/srcs - as a tree artifact.

aspect_rules_py's py_binary launcher creates its own venv as a sibling inside its own runfiles
directory the first time it runs, not as a declared build output, so nothing in the ordinary build
graph captures it - py_image_layer included, since it only packages whatever the build graph
already produced. This runs that launcher as an action instead and copies the resulting runfiles
directory out with symlinks dereferenced (`cp -aL`), the same fix that makes @katydid//release:
katydid portable: a relocated copy can't rely on symlinks back into the action's own sandbox or
external-repo cache.
"""

def _warm_py_binary_impl(ctx):
    warmed = ctx.actions.declare_directory(ctx.label.name)
    files_to_run = ctx.attr.binary[DefaultInfo].files_to_run
    binary = files_to_run.executable
    runfiles_dir = binary.path + ".runfiles"
    ctx.actions.run_shell(
        tools = [files_to_run],
        outputs = [warmed],
        command = "'{binary}' >/dev/null && cp -aL '{runfiles}/.' '{out}/'".format(
            binary = binary.path,
            runfiles = runfiles_dir,
            out = warmed.path,
        ),
        mnemonic = "WarmPyBinaryVenv",
        progress_message = "Warming venv for %s" % ctx.attr.binary.label,
    )
    return [DefaultInfo(files = depset([warmed]))]

warm_py_binary = rule(
    implementation = _warm_py_binary_impl,
    attrs = {
        "binary": attr.label(
            executable = True,
            cfg = "exec",
            mandatory = True,
            doc = "The py_binary target whose venv this warms and whose runfiles this captures.",
        ),
    },
)
