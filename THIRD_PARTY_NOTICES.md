# Third-party notices

Oto's repository source release includes two modified third-party Rust crates
because the validated dependency graph cannot currently be reproduced from
crates.io alone.

## davey 0.1.4

- Upstream project: <https://github.com/Snazzah/davey>
- Upstream commit: `a1e2e741bea06bc3b7167a5c3792844b8975993c`
- License: MIT
- Copyright: 2025-present Snazzah
- Included license: [`vendor/davey/LICENSE`](vendor/davey/LICENSE)
- Patch record: [`vendor/davey/OTO_PATCHES.md`](vendor/davey/OTO_PATCHES.md)

## openmls_rust_crypto 0.5.1

- Upstream project: <https://github.com/openmls/openmls>
- License: MIT
- Copyright: 2020 OpenMLS Authors
- Included license:
  [`vendor/openmls_rust_crypto/LICENSE`](vendor/openmls_rust_crypto/LICENSE)
- Patch record:
  [`vendor/openmls_rust_crypto/OTO_PATCHES.md`](vendor/openmls_rust_crypto/OTO_PATCHES.md)

The license expression in Oto's first-party package manifests does not replace
or broaden the licenses of these vendored works. Their license texts and
attributions remain included with their source.
