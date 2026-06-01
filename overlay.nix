inputs:
[
  (import inputs.rust-overlay)
  (final: prev: {
    bundler = prev.bundler.override { ruby = final.ruby_4_0; };
    bundix = prev.bundix.overrideAttrs (oldAtts: {
      ruby = final.ruby_4_0;
    });
    craneLib = (inputs.crane.mkLib final).overrideToolchain final.rust-bin.stable.latest.default;
    rust-toolchain = prev.rust-bin.stable.latest.default;
    # This is an extended rust toolchain with `rust-src` since that's required for IDE stuff
    rust-dev-toolchain = prev.rust-bin.stable.latest.default.override {
      extensions = [ "rust-src" ];
    };
  })
]
