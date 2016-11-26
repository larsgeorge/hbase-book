An Ansible role for the EPEL Yum repositiories.

# Usage

To use, setup your role like this:

```yaml
    ---
    - hosts: all
      remote_user: root
      roles:
        - goozbach.EPEL
```

# Options

## Disabling repos

Enable or disable `epel-testing` or `epel` repositories like this:

-  `epel_testing_enabled` is `0` by default
-  `epel_enabled` is `1` by default

```yaml
    ---
    - hosts: all
      remote_user: root
      roles:
        - { role: goozbach.EPEL, epel_testing_enabled: 1 }
        - { role: goozbach.EPEL, epel_enabled: 0 }
```

To use a disabled repo using the `yum` module use this syntax:

    - name: install the latest version of Apache from the testing repo
      yum: name=httpd enablerepo=epel state=installed

## EPEL Release mode
Change state of `epel-release` rpm (default is `installed`, change to `latest` to get an updated rpm):

```yaml
    ---
    - hosts: all
      remote_user: root
      roles:
        - { role: goozbach.EPEL, epel_state: latest }
```

Copyright (c) Derek Carter <derek@goozbach.com>

Licensed either under MIT or GPL version 2.0
See `LICENSE` for details.
