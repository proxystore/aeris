## Issues

[Issue Tracker](https://github.com/foobar-author/foobar/issues){target=_blank}

We use GitHub issues to report problems, request and track changes, and discuss
future ideas.
If you open an issue for a specific problem, please follow the template guides.

## Pull Requests

We use the standard GitHub contribution cycle where all contributions are
made via pull requests (including code owners!).

1. Fork the repository and clone to your local machine.
2. Create local changes.
    - Changes should conform to the style and testing guidelines, referenced
      above.
    - Preferred commit message format ([source](https://cbea.ms/git-commit/){target=_blank}):
        * separate subject from body with a blank line,
        * limit subject line to 50 characters,
        * capitalize first word of subject line,
        * do not end the subject line with a period,
        * use the imperative mood for subject lines,
        * include related issue numbers at end of subject line,
        * wrap body at 72 characters, and
        * use the body to explain what/why rather than how.
      Example: `Fix concurrency bug in Store (#42)`
3. Push commits to your fork.
    - Please squash commits fixing mistakes to keep the git history clean.
      For example, if commit "b" follows commit "a" and only fixes a small typo
      from "a", please squash "a" and "b" into a single, correct commit.
      This keeps the commit history readable and easier to search through when
      debugging (e.g., git blame/bisect).
4. Open a pull request in this repository.
    - The pull request should include a description of the motivation for the
      PR and included changes. A PR template is provided to guide this process.
