# Oto patch to openmls_rust_crypto 0.5.1

Upstream source is the MIT-licensed `openmls_rust_crypto 0.5.1` crate from the
OpenMLS project.

Oto updates its three HPKE dependencies from the compatible 0.6 line to 0.7.0.
The provider source is unchanged and compiles against the new API. This removes
the vulnerable `libcrux-sha3 0.0.8` graph in favor of 0.0.10 and also advances
the optional libcrux graph to versions containing the current security fixes.

Removal trigger: replace this patch with an upstream OpenMLS/davey release that
uses HPKE 0.7 or newer and passes Oto's DAVE conformance suite.
