{ pkgs, lib, ... }:

# Developer environment for the parquet-ruby gem. This provides the same tools
# as the dev shell in flake.nix: Ruby 4.0 to build/run the gem, a Rust toolchain
# with rust-src/rust-analyzer for the native extension and IDE support, and the
# native libraries the extension links against (duckdb, jemalloc, pkg-config).

{
  languages.ruby = {
    enable = true;
    package = pkgs.ruby_4_0;
  };

  # Rust from nixpkgs (devenv's default channel). This intentionally differs from
  # flake.nix, which builds the toolchain via rust-overlay: on aarch64-darwin
  # rust-overlay's component aggregation fails to merge the librustc_driver dylib
  # (`cp: ... are the same file`), so that path does not build here. The nixpkgs
  # toolchain is a plain symlinkJoin of cached, multi-output packages and builds
  # cleanly. It is a recent stable Rust, sufficient for the rb-sys/magnus build.
  # devenv sets RUST_SRC_PATH to rustPlatform.rustLibSrc and adds rust-analyzer.
  languages.rust = {
    enable = true;
    channel = "nixpkgs";
  };

  packages = with pkgs; [
    duckdb
    jemalloc
    pkg-config
  ];

  # rb-sys treats a Nix shell as CI and forces a release build. Setting this
  # makes it honor RB_SYS_CARGO_PROFILE, so a local `rake compile` can build a
  # debug profile during development.
  env.RB_SYS_TEST = "1";

  # This project builds on stable Rust. Override any inherited RUSTFLAGS (a
  # global `-Z threads=N` and other nightly-only flags are common in shells)
  # that stable rustc would reject with "the option `Z` is only accepted on the
  # nightly compiler", breaking `rake compile`. mkForce is required because
  # languages.rust also defines env.RUSTFLAGS (as null), and the two plain
  # definitions cannot be merged.
  env.RUSTFLAGS = lib.mkForce "";

  # Load repo-local .env, matching the old direnv `dotenv` call.
  dotenv.enable = true;
}
