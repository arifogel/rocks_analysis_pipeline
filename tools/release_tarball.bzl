"""release_tarball(): assembles this repo's own release tarball in a single action, so the
warmed venv is only ever materialized once - not once as an intermediate warmed-venv artifact and
again inside a separate final-assembly step.

Makes a portable copy of binary's own runfiles (dereferencing, `cp -aL`, before running: Bazel's
own runfiles symlinks point into the action's sandbox or external-repo cache, never valid once
relocated - the same fix that makes @katydid//release:katydid portable), runs the copied launcher
against that copy to force its lazily-created venv into existence there, lays extra_files (release
wrapper scripts, run_via_warmed_runfiles.sh) alongside under release_venv_warmed/'s own parent
directory, and tars the whole thing as this target's one output. Since the copy is already
portable before binary ever runs, whatever symlinks venv creation itself adds (its interpreter,
its own multiple bin/pythonX names) point within that same local tree and need no further
dereferencing - left as symlinks in the final tar, rather than dereferencing the whole tree a
second time, so the bytes of any file several of the venv's own symlinks point at aren't
duplicated.
"""

def _release_tarball_impl(ctx):
    out_tar = ctx.actions.declare_file(ctx.label.name + ".tar.gz")
    files_to_run = ctx.attr.binary[DefaultInfo].files_to_run
    binary = files_to_run.executable
    runfiles_dir = binary.path + ".runfiles"
    name = binary.basename
    extra_files = ctx.files.extra_files
    extra_paths = " ".join(["'%s'" % f.path for f in extra_files])
    ctx.actions.run_shell(
        tools = [files_to_run],
        inputs = extra_files,
        outputs = [out_tar],
        command = """
set -euo pipefail
STAGE="$(mktemp -d)"
cp -aL '{binary}' "$STAGE/{name}"
cp -aL '{runfiles}' "$STAGE/{name}.runfiles"
"$STAGE/{name}" >/dev/null
rm -f "$STAGE/{name}"
mv "$STAGE/{name}.runfiles" "$STAGE/release_venv_warmed"
for f in {extra}; do
  cp "$f" "$STAGE/"
done
tar czf '{out}' -C "$STAGE" .
rm -rf "$STAGE"
""".format(
            binary = binary.path,
            runfiles = runfiles_dir,
            name = name,
            extra = extra_paths,
            out = out_tar.path,
        ),
        mnemonic = "AssembleReleaseTarball",
        progress_message = "Assembling release tarball from %s" % ctx.attr.binary.label,
    )
    return [DefaultInfo(files = depset([out_tar]))]

release_tarball = rule(
    implementation = _release_tarball_impl,
    attrs = {
        "binary": attr.label(
            executable = True,
            cfg = "exec",
            mandatory = True,
            doc = "The umbrella py_binary whose venv this warms and whose runfiles this tars.",
        ),
        "extra_files": attr.label_list(
            allow_files = True,
            doc = "Extra files (wrapper scripts, run_via_warmed_runfiles.sh) to place alongside " +
                  "release_venv_warmed/ in the final tarball.",
        ),
    },
)
