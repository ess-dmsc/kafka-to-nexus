Breaking changes are tagged by `[breaking]`.

# 2017-08-07 e5fdb17

`[breaking]` Change option `broker-command` to `command-uri` to be better in
line with the new `status-uri`.


# 2017-08-04 fae8ae6

`[breaking]` Switch from `PCRE` to `std::regex`. This requires at least GCC
4.9.
