"""release_tarball(): assembles this repo's release tarball in a single action, materializing
the warmed venv only once.

Makes a portable copy of binary's runfiles before running it: Bazel's runfiles symlinks
point into the action's sandbox or external-repo cache, never valid once relocated. Runs the
copied launcher against that copy to force its lazily-created venv into existence there, lays
extra_files (release wrapper scripts, run_via_warmed_runfiles.sh) alongside under
release_venv_warmed/'s parent directory, and tars the whole thing as this target's one output.

Venv creation (uv, under the hood) adds its own symlinks for the interpreter and for per-file
entries of individually-linked packages. These are absolute paths into the action's staging
directory, not relative ones. pyvenv.cfg's `home` line and bin/python end up pointing at that
now-gone staging directory once extracted elsewhere. Every symlink whose target lies inside that
staging directory is rewritten to a relative one before tarring, so it still resolves correctly
wherever the tarball ends up extracted; a symlink pointing outside the staging directory entirely
would be a real, separate problem (none found for this binary) and is left untouched here rather
than silently masked.
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
find "$STAGE" -type l -print0 | while IFS= read -r -d '' link; do
  target="$(readlink "$link")"
  case "$target" in
    "$STAGE"/*)
      dir="$(dirname "$link")"
      rel="$(realpath --relative-to="$dir" "$target")"
      ln -sfn "$rel" "$link"
      ;;
  esac
done
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
