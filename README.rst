======
taccjm
======

TACC Job Manager is a lightweight python library to interact with and manage 
HPC resources provided by the Texas Advanced Computing Center (TACC).

Description
===========

TACCJM manages ssh connections to TACC resoures to deploy applications, run 
jobs, and download/upload data. These connections to specific TACC resources are
maintained by a locally deployed server that exposes an API to access TACC 
resources via http endpoints. This gives the user several flexible and 
persistent methods to establish and maintain connections to TACC resources 
programmatically and without repeated 2-factor authentication. Furthermore the
application and job hierarchy makes it easier to create reproducible and 
shareable HPC workflows for research.

.. _pyscaffold-notes:

Note
====

This project has been set up using PyScaffold 4.0.2. For details and usage
information on PyScaffold see https://pyscaffold.org/.
