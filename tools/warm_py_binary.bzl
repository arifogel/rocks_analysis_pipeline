"""warm_py_binary(): runs a py_binary once at build time to force its lazily-created venv into
existence, then tars its whole runfiles tree - interpreter, packages, warmed venv, and the
binary's own data/srcs - into a single archive, not a tree artifact: this target's own output is
one file, not a build-visible copy of an entire venv's worth of loose files.

aspect_rules_py's py_binary launcher creates its own venv as a sibling inside its own runfiles
directory the first time it runs, not as a declared build output, so nothing in the ordinary build
graph captures it - py_image_layer included, since it only packages whatever the build graph
already produced. This runs that launcher as an action instead and tars the resulting runfiles
directory with symlinks dereferenced (`tar --dereference`), the same fix that makes
@katydid//release:katydid portable: a relocated copy can't rely on symlinks back into the action's
own sandbox or external-repo cache.
"""

def _warm_py_binary_impl(ctx):
    warmed_tar = ctx.actions.declare_file(ctx.label.name + ".tar")
    files_to_run = ctx.attr.binary[DefaultInfo].files_to_run
    binary = files_to_run.executable
    runfiles_dir = binary.path + ".runfiles"
    ctx.actions.run_shell(
        tools = [files_to_run],
        outputs = [warmed_tar],
        command = "'{binary}' >/dev/null && tar --dereference -cf '{out}' -C '{runfiles}' .".format(
            binary = binary.path,
            runfiles = runfiles_dir,
            out = warmed_tar.path,
        ),
        mnemonic = "WarmPyBinaryVenv",
        progress_message = "Warming venv for %s" % ctx.attr.binary.label,
    )
    return [DefaultInfo(files = depset([warmed_tar]))]

warm_py_binary = rule(
    implementation = _warm_py_binary_impl,
    attrs = {
        "binary": attr.label(
            executable = True,
            cfg = "exec",
            mandatory = True,
            doc = "The py_binary target whose venv this warms and whose runfiles this tars.",
        ),
    },
)
