## Release Timeline

Releases are created on an as-needed basis.
Milestones are the [Issue Tracker](https://github.com/foobar-author/foobar/issues){target=_blank} are used to track features to be included in upcoming releases.

## Creating Releases

1. Choose the next version number, referred to as `{VERSION}` for the
   rest of the instructions. Versioning follows semver
   (`major.minor.patch`) with optional [PEP-440](https://peps.python.org/pep-0440){target=_blank}
   pre-release/post-release/dev-release segments. Major/minor/patch numbers
   start at 0 and pre-release/post-release/dev-release segments start at 1.
2. Update the version in `pyproject.toml` to `{VERSION}`.
3. Commit and merge the version updates/changelogs into main.
4. Tag the release commit and push (typically this is the commit updating the
   version numbers).
   ```bash
   $ git tag -s v{VERSION} -m "FooBar v{VERSION}"
   $ git push origin v{VERSION}
   ```
   Note the version number is prepended by "v" for the tags so we can
   distinguish release tags from non-release tags.
5. Create a new release on GitHub using the tag. The title should be
   `FooBar v{VERSION}`.
6. **Official release:**
    1. Use the "Generate release notes" option and set the previous tag as the previous official release tag. E.g., for `v0.4.1`, the previous release tag should be `v0.4.0` and NOT `v0.4.1a1`.
    2. Add an "Upgrade Steps" section at the top (see previous releases for examples).
    3. Review the generated notes and edit as needed. PRs are organized by tag, but some PRs will be missing tags and need to be moved from the "Other Changes" section to the correct section.
    4. Select "Set as the latest release."
7. **Unofficial release:** (alpha/dev builds)
    1. Do NOT generate release notes. The body can be along the lines of "Development pre-prelease for `V{VERSION}`."
    2. Leave the previous tag as "auto."
    3. Select "Set as a pre-release."
