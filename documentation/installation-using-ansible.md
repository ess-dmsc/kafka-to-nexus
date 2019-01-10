# Installation using Ansible

Install using the playbook:

```
ansible-playbook -i hosts kafka-to-nexus.yml
```

The filewriter can be installed using the ansible playbook defined in
`ansible`. The file `roles/kafka-to-nexus/defaults/main.yml` defines
the variables used during installation. The variables `<dep>_src` and
`<dep>_version` are the remote source and the required version of the
dependency, `<dep>` the install location. The sources and builds of
the dependencies are kept in `sources` and `builds`.

`filewriter_inc`, `filewriter_lib` and `filewriter_bin` defines
`CMAKE_INCLUDE_PATH`, `CMAKE_LIBRARY_PATH` and `CMAKE_PROGRAM_PATH`.

The default installation has the following structure

```
/opt/software/sources/<package1>-<version>
/opt/software/sources/<package2>-<version>
...
/opt/software/builds/<package1>-<version>
/opt/software/builds/<package2>-<version>
...
/opt/software/<package1>-<version>
/opt/software/<package2>-<version>
...
/opt/software/sources/filewriter-<version>
/opt/software/builds/filewriter-<version>
```
