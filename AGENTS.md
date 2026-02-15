# ADMapper

ADMapper is a frontend for BloodHound, visualizing Active Directory
permissions as a graph.

We are still in "greenfield project mode", as there is no release yet,
so never worry about backwards compatibility.

## Tech Stack and Philosophy

- Written in Rust
- CozoDB/SQLite DB backend
- HTMX and vanilla TypeScript for the frontend
- Tailwind CSS
- Focus on server-side rendering, except for diagrams
- Modern, intuitive and beautiful UI
- Snappy interface
- Obsessed with efficency and speed

## Features

- Tauri-based desktop app for Windows, Linux, MacOS.
- Optional web server with an API
- Light and dark theme
- Classic menu bar at the top

## Conventions

- Code should be readable, maintainable, and testable.
- Try to adhere to the DRY principle.
- Don't overly abstract. Let's be pragmatic.
- Let's stick to best practices and idiomatic patterns.
- We prefer functions to be less than 50 lines and files less than 1000
  lines, but it's not a hard limit.
- Functions should not have more than five positional arguments, but
  it's not a hard limit.
- In the HTML templates, try to work with macros wherever it makes
  sense. If a similar UI element appears three times, that's a strong
  signal to use macros. But if they are too different in subtle ways
  such that the macro would become awkward, refrain from macros. Note
  that sincs askama 0.15, macros can have bodies, which vastly improves
  composability.
- We value a smooth experience. Feedback to user actions should be
  instant. Actions that are expected to take more than around 400ms
  should use a spinner. Actions that are expected to take more than
  around five seconds should use a background job. Avoid flickering of
  the UI.

## Development

- Issues will be in `issues/new` in markdown files.
- After solving an issue, move the file to `issues/closed`.
- After solving an issue, create a git commit. In the commit message,
  focus on the "why" instead of "how". The "how" can be deduced from the
  diff. However, a short summary of the "how" can't hurt to convey
  intent.
- Before commiting, run linters, formatters, and the test suite.
- When fixing bugs, add test cases.
- When adding features, update the docs and/or README.

## Version Management

To bump the version:

1.  Run the version bump script with the new version number:

    ``` bash
    ./scripts/bump-version.sh X.Y.Z
    ```

2.  This script will:

    - Update `Cargo.toml`
    - Update `package.json`
    - Update `package-lock.json`
    - Update `Cargo.lock`
    - Create a git commit
    - Create a git tag `vX.Y.Z`

3.  Push the changes and tag to trigger the Docker build:

    ``` bash
    git push origin main
    git push origin vX.Y.Z
    ```

The version in `Cargo.toml` is the single source of truth. The health
endpoint automatically reads from it via `env!("CARGO_PKG_VERSION")`.

## Agents

If you are an LLM:

- use your identity as co-auther
- If you make changes to the UI, check with playwright for obvious
  visual problems, like elements running into each other. You can use
  `uv venv && uv pip install playwright` to install dependencies.
