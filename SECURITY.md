# Security policy

## Supported versions

Security fixes are provided for the latest Oto 1.x release.

## Reporting a vulnerability

Please use GitHub's private vulnerability reporting for this repository:

1. Open the repository's **Security** tab.
2. Select **Advisories** and then **Report a vulnerability**.
3. Include affected versions, impact, reproduction details, and any suggested
   mitigation without including real Discord credentials or captured media.

Do not open a public issue for a suspected vulnerability. If private reporting
is unavailable, open a public issue containing no vulnerability details and ask
the maintainer to establish a private contact channel.

Never submit bot tokens, voice tokens, session identifiers, transport keys,
DAVE key material, or decrypted/signed media. Revoke any credential that may
have been disclosed before sending a report.

## Dependency note

The locked graph currently contains the allowed `RUSTSEC-2026-0173` warning for
unmaintained `proc-macro-error2 2.0.1`. It is reached only through the
libcrux/hax build dependency graph and has no known vulnerability. It is
re-evaluated on dependency updates and should be removed when the validated
DAVE/OpenMLS graph no longer requires it.
