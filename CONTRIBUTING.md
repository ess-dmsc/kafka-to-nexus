# Contributing

We use `clang-format` v3.4 LLVM default style as discussed in the group.
Due to availability of `clang-format` on the different systems that we use
in development, we decided on this lowest common denominator.

The `.clang-format` is just a `-dump-config` of the default LLVM style from
`clang-format` v3.4.


## Branching

- Branch your feature off from 'dev'

- Before creating pull requests, rebase your feature branch
  - Reorder and squash small successive commits which may have occurred
    during iterative development to improve readability of the feature
    branch.

- Create pull requests against 'dev'

- For important bug fixes, branch off 'master' to create a bug-fix-only
  feature branch.
