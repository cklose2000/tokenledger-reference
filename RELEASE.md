# Release preparation

Prepared 2026-09-07 as candidate `v0.2.0-rc.4`. These commits reconstruct reviewable groups
from an existing implementation at preparation time; they do not claim an
earlier public development timeline. Intermediate assembly commits are not
individually runnable releases. No original private Git objects are copied.

The allowlist includes generic runtime, definitions, SQL, independent baseline
and selected synthetic regression tests. It excludes all original ledgers,
generated reports, personal evidence, private application configuration, hiring
strategy, private module plans and working development instructions.

Source file bytes are selected from committed Git blobs, never arbitrary working
tree contents. Release receipt fixtures must be regenerated under these public
revisions, and then verified from an isolated clone of this repository. Original
private receipt IDs and private SHAs are not public reproduction evidence.

The image pins the Python version used by retained local replay. Image changes
need fresh dependency, semantic and replay verification. See the image digest
in `.devcontainer/Dockerfile`; see the package for exact engine pins.

Verification statuses start as not_run. Populate a verification artifact from
actual commands before claiming a clean installation, hosted environment,
challenge score, timing, agent comparison or cloud acceptance. This preparation
command performs no publication, account access or cloud operation.
