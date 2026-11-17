<!--
Delete this comment before requesting review. The title and this description
become the squash-merge commit on main, and the title is the changelog entry.

Title (checked in CI, selects the release bump):

  <type>[optional scope]: <description>

Types, scopes, and version bumps:
https://nasa-fornax.github.io/fornax-cutouts/contributing

Link issues with "Fixes #123" when this closes them.
Do not edit CHANGELOG.md.

Incompatible behavior or public API changes need a footer on its own line at
the end of this description: the words BREAKING CHANGE, a colon, then what
changed and how to migrate. semantic-release reads that footer.
-->

## Summary

<!-- What changed and why. For user-facing changes, name the API, config, or job behavior affected. -->

## Test plan

<!-- What you ran, or what a reviewer should check. CI runs pre-commit and pytest. -->

## Checklist

- [ ] Tests cover the behavior change, or this change has no runtime behavior
- [ ] Docs updated when behavior, configuration, or the public API changed
- [ ] Incompatible changes include a breaking-change footer and a migration note
