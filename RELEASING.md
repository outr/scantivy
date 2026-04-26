# Releasing Scantivy

This document describes how to cut a Scantivy release to Maven Central via Sonatype.

## Prerequisites

GitHub repository secrets must be set:

- `PGP_SECRET` — base64-armored private signing key
- `PGP_PASSPHRASE` — passphrase for that key
- `SONATYPE_USERNAME` — Sonatype Central token username
- `SONATYPE_PASSWORD` — Sonatype Central token password

**Version sources of truth:**
- **Scala / published artifact:** derived automatically from the git tag by [sbt-dynver](https://github.com/sbt/sbt-dynver). Tag `v1.2.3` → published version `1.2.3`. No manual bump needed.
- **`rust/Cargo.toml`:** manually maintained. Must equal the tag (without the `v` prefix). The release workflow guards against drift and refuses to publish on mismatch.

## Recommended pre-flight: dry-run with an rc tag

Before tagging the final release, dry-run with an rc:

```bash
# 1. Bump Cargo.toml to the rc version (build.sbt has no `version :=` line — sbt-dynver
#    derives it from the tag).
sed -i 's/^version = ".*"$/version = "1.0.0-rc1"/' rust/Cargo.toml

# 2. Commit and push the bump.
git add rust/Cargo.toml
git commit -m "rc1"
git push

# 3. Tag and push.
git tag -a v1.0.0-rc1 -m "v1.0.0-rc1"
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
# 1. Bump Cargo.toml to the final version.
sed -i 's/^version = ".*"$/version = "1.0.0"/' rust/Cargo.toml

# 2. Commit, push, tag.
git add rust/Cargo.toml
git commit -m "Release 1.0.0"
git push
git tag -a v1.0.0 -m "v1.0.0"
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
2. Leave `Cargo.toml`'s version at the just-released number — sbt-dynver on the Scala side
   automatically reports `<version>+<commits-ahead>-<sha>-SNAPSHOT` for post-release commits, and
   Cargo.toml is internal-only (the crate isn't published to crates.io), so a static "current
   major" placeholder is fine. Bump it again as part of the next release commit.
