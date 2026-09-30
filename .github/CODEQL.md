# CodeQL configuration

This repository uses GitHub CodeQL **default setup**.

Do not add a workflow that runs `github/codeql-action/analyze` while default setup
is enabled. GitHub rejects SARIF uploaded by an advanced-setup workflow when
default setup is active, even when analysis itself completes successfully.

If the repository needs a custom build or query configuration in the future,
disable default setup in the repository's code-scanning settings before adding an
advanced-setup workflow.
