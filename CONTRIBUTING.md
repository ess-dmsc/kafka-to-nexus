# Contributing

We use `clang-format` v3.9 LLVM default style.
The `.clang-format` is just a `-dump-config` of the default LLVM style from
`clang-format` v3.9.

Please refer to [LLVM documentation](https://llvm.org/docs/CodingStandards.html).

## Issues 

For issues that require changes in the functionality of the code (i.e. not tiny bits of refactoring) create a ticket on the [ESS JIRA board](https://jira.esss.lu.se/secure/RapidBoard.jspa?rapidView=167&view=detail&quickFilter=2154) with the label `FW&FW`

## Branching

- Branch your feature off from 'master'

- Before creating pull requests, rebase your feature branch
  - Reorder and squash small successive commits which may have occurred
    during iterative development to improve readability of the feature
    branch.

- Create pull requests against 'master'.

## Naming conventions

- Branch: `issuenumber_issue_decription`, e.g. `314_throw_error_if_branch_name_invalid`

- Commits: `re #issuenumber commit message`, e.g. `re #42 remove dead code`

