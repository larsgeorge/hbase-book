# HBase Cluster Setup

This repository contains the necessary (Ansible)[https://www.ansible.com/] playbooks to set up the test cluster used while writing the "HBase - The Definitive Guide" (2. Edition) book. 

The use is as such: there is an *install* playbook, and a *services* playbook. The former does an idempotent setup of the software itself and configures the services, with or without security. The latter allows to start and stop the services and is implicitly invoked by the former. Both are explained next.

## Installation Playbook

Ansible is configuration management system that executes playbooks, others call them recipes, on remote hosts. A common concept with these systems is that they work best in practice when the contained steps are repeatable, or otherwise called idempotent. This allows for an administrator to repeatedly execute the playbook (for example, after something was added or changed) and end up with a working cluster. Ansible supports the notion of skipping steps that are not necessary, which is used for the installation playbook where possible.

The playbook is executed like this, in its most basic form:

```
$ ansible-playbook install.yml -b -u hbasebook -i inventories/hbasebook.inv
```

Note that `hbasebook` is the technical user account used to set up the cluster. 

You can customize what you want to set up by adding tags to the command, for example:

```
$ ansible-playbook services.yml -b -u hbasebook -i inventories/hbasebook.inv --tags "hbase"
```

This would only apply the steps needed for HBase.

## Services Playbook

The services playbook start and/or stops the various services. Called without further options it restarts all services:

```
$ ansible-playbook services.yml -b -u hbasebook -i inventories/hbasebook.inv
```

Like with the above you can customize the call by adding tags, such as `start` to start all services. To just restart HBase do this:
 
```
$ ansible-playbook services.yml -b -u hbasebook -i inventories/hbasebook.inv --tags "stop-hbase,start-hbase"
```
