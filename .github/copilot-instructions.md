# Copilot Instructions (Submodule: sck-core-execute)

- Tech: Python package (execution engine).
- Precedence: Local first; then root docs at `../../.github/`.
- Conventions: See `../sck-core-ui/docs/backend-code-style.md` for AWS/S3/Lambda patterns.

## Google Docstring Requirements
**MANDATORY**: All docstrings must use Google-style format for Sphinx documentation generation:
- Use Google-style docstrings with proper Args/Returns/Example sections
- Napoleon extension will convert Google format to RST for Sphinx processing
- Avoid direct RST syntax (`::`, `:param:`, etc.) in docstrings - use Google format instead
- Example sections should use `>>>` for doctests or simple code examples
- This ensures proper IDE interpretation while maintaining clean Sphinx documentation

## Contradiction Detection
- Cross-check backend patterns and root precedence.
- If conflict, warn + options + example.
- Example: "Long-running sync calls conflict with Lambda execution model; use async or step functions patterns as documented."

## Standalone clone note
If cloned standalone, see:
- UI/backend conventions: https://github.com/eitssg/simple-cloud-kit/tree/develop/sck-core-ui/docs
- Root Copilot guidance: https://github.com/eitssg/simple-cloud-kit/blob/develop/.github/copilot-instructions.md
 
