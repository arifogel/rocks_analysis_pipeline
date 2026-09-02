"UV toolchain support"

def _uv_repo_impl(ctx):
    version = ctx.attr.version

    VERSIONS = {
        "0.11.21": {
            "aarch64-apple-darwin": ("uv-aarch64-apple-darwin.tar.gz", "1f921d491ba5ffeea774eb04d6681ecee379101341cbb1500394993b541bf3f4"),
            "x86_64-apple-darwin": ("uv-x86_64-apple-darwin.tar.gz", "f3c8e5708a84b920c18b691214d54d2b0da6b984789caae95d47c95120cb7765"),
            "aarch64-pc-windows-msvc": ("uv-aarch64-pc-windows-msvc.zip", "74e443f8004022dde57a1bd0d10c097830f9ea8feb4ec927db52cd5d805c2f48"),
            "i686-pc-windows-msvc": ("uv-i686-pc-windows-msvc.zip", "77d7979222c6bd621bdb862c9cb138be41dce1e3cea239b1e87eb82dfac2dbd5"),
            "x86_64-pc-windows-msvc": ("uv-x86_64-pc-windows-msvc.zip", "ace861f360c6de2babedc1607d0f454b6b09a820dbc8182dc15af927e4df9589"),
            "aarch64-unknown-linux-gnu": ("uv-aarch64-unknown-linux-gnu.tar.gz", "88e800834007cc5efd4675f166eb2a51e7e3ad19876d85fa8805a6fb5c922397"),
            "i686-unknown-linux-gnu": ("uv-i686-unknown-linux-gnu.tar.gz", "07125219898b1c8e71bc612d91b190927c6b192a7bce5dd029b1c9070e9b7049"),
            "powerpc64le-unknown-linux-gnu": ("uv-powerpc64le-unknown-linux-gnu.tar.gz", "0e97021d831f9670c8261f9270ecf94b83f1a90ff5312389e37a77676deaec87"),
            "riscv64gc-unknown-linux-gnu": ("uv-riscv64gc-unknown-linux-gnu.tar.gz", "63013d7afe8fd552b273a7a5ca1f1425c0c82b12d73454d24237876bc26006e9"),
            "s390x-unknown-linux-gnu": ("uv-s390x-unknown-linux-gnu.tar.gz", "743694a86a05eaf15d292c3d757388c4b2a11b4a7eb67f000077b4d6c467347e"),
            "x86_64-unknown-linux-gnu": ("uv-x86_64-unknown-linux-gnu.tar.gz", "8c88519b0ef0af9801fcdee419bbb12116bd9e6b18e162ae093c932d8b264050"),
            "armv7-unknown-linux-gnueabihf": ("uv-armv7-unknown-linux-gnueabihf.tar.gz", "929440f991ccd8097e01be1ec831f673ac7bbf508e94819b4270f2873f69e658"),
            "aarch64-unknown-linux-musl": ("uv-aarch64-unknown-linux-musl.tar.gz", "e71badaed2a2c3a404a0a00974b51c7ed5f5bc7be947916846005b739c68a5a2"),
            "i686-unknown-linux-musl": ("uv-i686-unknown-linux-musl.tar.gz", "865eff26cef62b7862854e176d57d9e0164daeec595723132a81aa3611238798"),
            "riscv64gc-unknown-linux-musl": ("uv-riscv64gc-unknown-linux-musl.tar.gz", "b869fe80435715b2b414443af28de96ed5d7f8c6759e12ba141abca221ebc0cd"),
            "x86_64-unknown-linux-musl": ("uv-x86_64-unknown-linux-musl.tar.gz", "9dadff5b9e7b1d2d011e41852a1cbca713d9d5d88194f2eb6bd240fa4fb0a719"),
            "arm-unknown-linux-musleabihf": ("uv-arm-unknown-linux-musleabihf.tar.gz", "7cd6637deebacfa0224e53afb4dd7da4f464ba0ecc128f6f543897c157e39a0f"),
            "armv7-unknown-linux-musleabihf": ("uv-armv7-unknown-linux-musleabihf.tar.gz", "20f4b653a17adb09cdfa7f911d46a1f254b918a2b49bef1266c735ab4c6fced0"),
        },
    }

    # Detect platform and normalize architecture
    os_lower = ctx.os.name.lower()
    arch = ctx.os.arch
    if arch == "amd64":
        arch = "x86_64"
    elif arch == "arm64":
        arch = "aarch64"

    # Simple lookup logic based on known release formats
    if "mac os" in os_lower:
        platform_key = "{}-apple-darwin".format(arch)
    elif "linux" in os_lower:
        platform_key = "{}-unknown-linux-gnu".format(arch)
    else:  # windows
        platform_key = "{}-pc-windows-msvc".format(arch)

    file_name, sha = VERSIONS[version][platform_key]
    url = "https://github.com/astral-sh/uv/releases/download/{}/{}".format(version, file_name)

    ctx.download_and_extract(url, sha256 = sha)
    dir_name = file_name.rsplit(".", 2)[0]

    # Expose both binaries
    ctx.file("BUILD.bazel", """
package(default_visibility = ["//visibility:public"])

filegroup(
    name = "uv",
    srcs = select({{
        "@bazel_tools//src/conditions:windows": ["{dir}/uv.exe"],
        "//conditions:default": ["{dir}/uv"],
    }}),
)

filegroup(
    name = "uvx",
    srcs = select({{
        "@bazel_tools//src/conditions:windows": ["{dir}/uvx.exe"],
        "//conditions:default": ["{dir}/uvx"],
    }}),
)
""".format(dir = dir_name))

uv_repository = repository_rule(
    implementation = _uv_repo_impl,
    attrs = {"version": attr.string(mandatory = True)},
)
