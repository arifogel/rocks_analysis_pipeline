"""release_wrapper(): generates one entry point's own release-tarball wrapper script."""

def release_wrapper(name, venv_target_name, script_target_name):
    """Generates a genrule producing "<script_target_name>.sh": a wrapper that activates
    venv_target_name's own shared, warmed venv and execs script_target_name in it, meant to sit
    alongside release_venv_warmed/ and run_via_warmed_runfiles.sh itself in the assembled release
    tarball.

    Args:
      name: this genrule's own target name.
      venv_target_name: run_via_warmed_runfiles.sh's own venv_target_name argument.
      script_target_name: run_via_warmed_runfiles.sh's own script_target_name argument.
    """
    native.genrule(
        name = name,
        outs = [script_target_name + ".sh"],
        cmd = ("""cat > $@ << 'WRAPPER_EOF'
#!/usr/bin/env bash
set -euo pipefail
DIR="$$(cd "$$(dirname "$${BASH_SOURCE[0]}")" && pwd)"
RUNFILES_DIR="$$DIR/release_venv_warmed" exec "$$DIR/run_via_warmed_runfiles.sh" %s %s "$$@"
WRAPPER_EOF
chmod +x $@
""" % (venv_target_name, script_target_name)),
    )
