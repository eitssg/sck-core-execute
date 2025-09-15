# Copilot Instructions (Submodule: sck-core-execute)

- Tech: Python package (execution engine).
- Precedence: Local first; then root docs at `../../.github/`.
- Conventions: See `../sck-core-ui/docs/backend-code-style.md` for AWS/S3/Lambda patterns.

## Contradiction Detection
- Cross-check backend patterns and root precedence.
- If conflict, warn + options + example.
- Example: "Long-running sync calls conflict with Lambda execution model; use async or step functions patterns as documented."

## Standalone clone note
If cloned standalone, see:
- UI/backend conventions: https://github.com/eitssg/simple-cloud-kit/tree/develop/sck-core-ui/docs
- Root Copilot guidance: https://github.com/eitssg/simple-cloud-kit/blob/develop/.github/copilot-instructions.md
 
