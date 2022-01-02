======
taccjm
======

TACC Job Manager is a lightweight python library for managing
HPC resources provided by the Texas Advanced Computing Center (TACC).

Description
===========

TACCJM manages ssh connections to TACC systems for deploying applications,
running jobs, and downloading/uploading data. These connections to specific
TACC resources are maintained by a locally deployed server that exposes an
API to access TACC resources via http endpoints. This gives the user several
methods to establish and maintain connections to TACC resources
programmatically and without repeated 2-factor authentication.
Furthermore the application and job hierarchy makes it easier to create
reproducible and shareable HPC workflows for research.

.. _pyscaffold-notes:

Requirements
============

Using taccjm requires a TACC account enable with 2-fa. In order to run jobs
on TACC systems as well, you will need a valid allocation associated with your
user ID. Finally, your TACC user account needs to have 2-factor authentication
enabled. See the [TACC user portal](https://portal.tacc.utexas.edu/)

Installation
============

To install use pip:

```
pip install taccjm
```

A docker image for taccjm is in development and will hopefully be available in
a future release.

Getting Started
===============

The easiest way to use taccjm is through


Note
====

This project has been set up using PyScaffold 4.0.2. For details and usage
information on PyScaffold see https://pyscaffold.org/.
