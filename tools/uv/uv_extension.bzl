"UV extension support"

load(":uv_toolchain.bzl", "uv_repository")

def _uv_impl(ctx):
    for mod in ctx.modules:
        for attr in mod.tags.version:
            # This triggers the repository rule for every version defined in MODULE.bazel
            uv_repository(name = "uv_bin", version = attr.version)

uv_ext = module_extension(
    implementation = _uv_impl,
    tag_classes = {
        "version": tag_class(attrs = {"version": attr.string(mandatory = True)}),
    },
)
