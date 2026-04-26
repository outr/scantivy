# Releasing Scantivy

This document describes how to cut a Scantivy release to Maven Central via Sonatype.

## Prerequisites

GitHub repository secrets must be set:

- `PGP_SECRET` — base64-armored private signing key
- `PGP_PASSPHRASE` — passphrase for that key
- `SONATYPE_USERNAME` — Sonatype Central token username
- `SONATYPE_PASSWORD` — Sonatype Central token password

Both `Cargo.toml` and `build.sbt` must declare the same version string. Currently both are at
`1.0.0`.

## Recommended pre-flight: dry-run with an rc tag

The publish pipeline has never run end-to-end. Before tagging the final release, dry-run with an
rc:

```bash
# 1. Bump both versions to a release-candidate.
sed -i 's/^version = "1.0.0"$/version = "1.0.0-rc1"/' rust/Cargo.toml
sed -i 's/^version := "1.0.0"$/version := "1.0.0-rc1"/' scala/build.sbt

# 2. Commit and push the bump.
git add rust/Cargo.toml scala/build.sbt
git commit -m "rc1"
git push

# 3. Tag and push.
git tag v1.0.0-rc1
git push origin v1.0.0-rc1
```

Pushing the `v*` tag triggers `.github/workflows/release.yml`, which:

1. Matrix-builds native libs for `linux-x86_64`, `linux-aarch64`, `macos-aarch64`, `macos-x86_64`,
   and `windows-x86_64`.
2. Stages them under `scala/src/main/resources/native/<os>-<arch>/`.
3. Imports the PGP key, runs `sbt publishSigned; sonatypeBundleRelease`.

Verify on Sonatype Central's staging UI that the bundle is correct (5 native libs present, jar
size sane, signatures valid, POM well-formed). Pull the rc artifact in a downstream project and
run a quick smoke test on each platform you care about.

## Cutting the final release

Once the rc validates:

```bash
# 1. Bump back to the final version.
sed -i 's/^version = "1.0.0-rc1"$/version = "1.0.0"/' rust/Cargo.toml
sed -i 's/^version := "1.0.0-rc1"$/version := "1.0.0"/' scala/build.sbt

# 2. Commit, push, tag.
git add rust/Cargo.toml scala/build.sbt
git commit -m "Release 1.0.0"
git push
git tag v1.0.0
git push origin v1.0.0
```

The same workflow runs on the final tag.

## Local sanity checks before tagging

```bash
# Rust
cd rust
cargo fmt --all -- --check
cargo clippy --release --all-targets -- -D warnings
cargo test --release

# Scala (after copying the host's libscantivy into scala/lib/)
cd ../scala
sbt test
```

All four must pass before pushing a release tag.

## After release

1. Update `CHANGELOG.md` with the released version's date if it wasn't already correct.
2. Bump versions to the next `-SNAPSHOT` so subsequent commits target the next release.
