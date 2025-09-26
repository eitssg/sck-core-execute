# Copilot Instructions (Submodule: sck-core-execute)

- Tech: Python package (execution engine).
- Precedence: Local first; then root docs at `../../.github/`.
- Conventions: See `../sck-core-ui/docs/backend-code-style.md` for AWS/S3/Lambda patterns.

## RST Documentation Requirements
**MANDATORY**: All docstrings must be RST-compatible for Sphinx documentation generation:
- Use proper RST syntax: `::` for code blocks (not markdown triple backticks)
- Code blocks must be indented 4+ spaces relative to preceding text
- Add blank line after `::` before code content
- Bullet lists must end with blank line before continuing text
- Use RST field lists for parameters: `:param name: description`
- Use RST directives: `.. note::`, `.. warning::`, etc.
- Test docstrings with Sphinx build - code is source of truth, not docstrings

## Contradiction Detection
- Cross-check backend patterns and root precedence.
- If conflict, warn + options + example.
- Example: "Long-running sync calls conflict with Lambda execution model; use async or step functions patterns as documented."

## Standalone clone note
If cloned standalone, see:
- UI/backend conventions: https://github.com/eitssg/simple-cloud-kit/tree/develop/sck-core-ui/docs
- Root Copilot guidance: https://github.com/eitssg/simple-cloud-kit/blob/develop/.github/copilot-instructions.md
 
