# Instructions for releasing a new version

Deprecates release instruction from
[RFC 2](https://github.com/edgedb/rfcs/blob/master/text/0002-edgedb-release-process.rst).

EdgeDB packages are published on https://packages.edgedb.com.
They are build in GitHub Actions pipelines, using
https://github.com/edgedb/edgedb-pkg.

Releases are built from a release branch associated with a major version
(i.e. "release/4.x"). At feature freeze, we create this branch. From that moment
on, all additional commits will have to be cherry-picked to this branch.

Before the major version, we publish "testing releases":

- "alpha" (i.e. `v4.0a1`, `v4.0a2`),
- "beta" (i.e. `v4.0b1`, `v4.0b2`),
- "release candidates" (`v4.0rc1`) that we might promote into the final release.

## Internal Communication

Announce on team slack when you are beginning to prepare a release,
when a release build has been kicked off, and when the release has
succeeded. Update the thread with any problems and attempted
resolutions.

Communicate in the other direction as well: make sure the release
manager knows of any pending work that you want in a release.

"b1", "rc1", and ".0" releases are big deals. Make sure to get signoff
before releasing.

## edgedb-ui

On release branches, `edgedb-ui` should be pinned to the associated branch.
This can be done in `setup.py` with the variable `EDGEDBGUI_COMMIT`.
For example, on branch `release/4.x`, it is pinned to `edgedb-ui`'s branch `4.x`.
This means any release off `release/4.x` will contain latest commits from
`edgedb-ui`'s branch `4.x`.


## Preparing commits for a release

For each major release `N`, we have two GitHub labels: `to-backport-N.x` and
`backported-N.x`. PRs that need to be backported should be labelled with
`to-backport-N.x` for each of the target versions.

Once a PR is backported, `to-backport-N.x` should be removed and
`backported-N.x` added.

Tracking both states makes it easy to tell what needs to be backported
and what has been backported.

(Historical note: previously we had simply a `backport-N.x` label.
This made it easy to ensure that everything that got labelled with
`backport` actually got backported, but there was not an at-a-glance
way to see if something *had* been backported. Even looking at the
issue didn't always tell you, since sometimes we labelled things as
`backport` and then thought better of it.)

### Technical helpers

The `gh` command line makes a bunch of these operations simple.

To enumerate all pending backports for a branch:
```bash
gh pr list --state all -l to-backport-5.x
```

To adjust labels to mark a PR as backported:
```bash
gh pr edit --remove-label to-backport-N.x --add-label backported-N.x <PR NUMBER>
```

A helper shell script to cherry-pick a commit using its PR number:

```bash
# this won't work if a PR is not squashed into a single commit
function cp-pr {
    git cherry-pick $(gh pr view $1 --json mergeCommit --jq .mergeCommit.oid)
}
```


### What to backport?
Sometimes, people will forget to label the PR to be back-ported, so a good
practice is to list all commits since the last release:

```
git show releases/4.x # to see the last commit that has been cherry-picked
git log master # find the hash of that commit on master
git log hash_of_that_commit..master > ../to-backport.txt
```

Now, one can go through the list and see if the commits are worth back-porting.
A few pointers:

- Don't backport new features, unless it is high-priority for some reason.
- Don't backport docs, since the website is built from master.
- Don't backport refactors, since they might introduce bugs and there is no
  point in improving the codebase of a branch we are not developing on anymore.
  Disregard this rule early on after the fork of the release branch, since
  porting refactors will decrease chances merge conflicts of other commits later
  on.
- Don't backport "build" commits (updating of build deps, refactoring of the
  release pipeline), since that might trigger problems in the release process.
- If a PR changes:

  - any of the schema objects (i.e. adding a field to `s_types.Type`) or
  - a std library object (i.e. changing implementation of `std::round`),
  - metaschema (i.e. changing a pg function `edgedb.range_to_jsonb`),
    ... a "patch" needs to be added into `pgsql/patches.py`.
    This is needed, because minor releases don't require a "dump and restore",
    so we must apply these changes to existing user databases.

  Patches must be tested using this GHA workflow:
  https://github.com/edgedb/edgedb/actions/workflows/tests-patches.yml

## Release pipeline

When you have your commits ready, tag the commit and push:

```
# git tag --sign v4.5
# git push origin releases/4.x --follow-tags
```

Then open GitHub Actions page and run one of these pipelines:

- https://github.com/edgedb/edgedb/actions/workflows/testing.yml
- https://github.com/edgedb/edgedb/actions/workflows/release.yml

This will kick-off an GHA workflow that should take ~3 hours.
It will build, test and publish for each of the supported platforms.
It will not publish any packages if any of the tests fail.

Sometimes, tests will be flakey and just need to be re-run.
You can do that with a button top-right.

## Changelog

Each major release has a changelog page in the docs (i.e.
`docs/changelog/4_x.rst`). It should contain explanations of the new features,
which are usually composed by our dev-rel team.

Each minor release is just a subsection in the page, as a list of back-ported
PRs. Any PRs that fix internal stuff (like our test framework) or are not user
facing should not be included in the changelog.

Don't forget to include commits released from `edgedb-ui`.

These changes need to land on master branch and are not needed on the release
branch, so best course of action if to open a PR to master after kicking off
the release pipeline. After that PR is merged, the website needs to be
deployed, for changelog to land on the website (ping dev rel team).

A helper function to generate changelog is:

```python
# I keep this in ../compose-changelog.py

import json
import requests
import re
import sys

BASE_URL = 'https://api.github.com/repos/edgedb/edgedb/compare'

def main():
    if len(sys.argv) < 2:
        print('pass a sha1 hash as a first argument')
        sys.exit(1)

    from_hash = sys.argv[1]
    if len(sys.argv) > 2:
        to_hash = sys.argv[2]

    r = requests.get(f'{BASE_URL}/{from_hash}...{to_hash}')
    data = json.loads(r.text)

    for commit in data['commits']:
        message = commit['commit']['message']
        first_line = message.partition('\n\n')[0]
        if commit.get('author'):
            username = '@{}'.format(commit['author']['login'])
        else:
            username = commit['commit']['author']['name']
        sha = commit["sha"][:8]

        m = re.search(r'\#(?P<num>\d+)\b', message)
        if m:
            issue_num = m.group('num')
        else:
            issue_num = None

        first_line = re.sub(r'\(\#(?P<num>\d+)\)', '', first_line)
        print(f'* {first_line}')
        # print(f'  (by {username} in {sha}', end='')
        if issue_num:
            print(f'  (:eql:gh:`#{issue_num}`)')
        print()

if __name__ == '__main__':
    main()
```

```bash
python ../compose-changelog.py v4.5 v4.6 >> docs/changelog/4_x.rst
```

## After the release

The release pipelines will make the new version available at
https://packages.edgedb.com. This is enough for it to be installable using the
CLI, but other methods of installation need to be kicked of manually:

- our cloud team needs to deploy separate _cloud wizardly groups_,
- docker image needs to be published to https://hub.docker.com,
- Digital Ocean image needs to be published by Frederick,
