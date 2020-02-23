fs\_dlk
=======

*pyfilesystem2 interface to Azure Datalake*


Installing
----------

::

    pip install fs-dlk


Opening FS Datalake
-------------------

Authentication with username and password:

.. code-block:: python

    from fs_dlk import DLKFS
    dlkfs = DLKFS("/path/to/the/remote/dir", username="username", password="password")


Authentication with tenant secret:

.. code-block:: python

    from fs_dlk import DLKFS
    dlkfs = DLKFS("/path/to/the/remote/dir", tenant_id="tenant id", client_id="client id", client_secret="client_secret")


Specifying custom store name:

.. code-block:: python

    from fs_dlk import DLKFS
    dlkfs = DLKFS("/path/to/the/remote/dir", store="store name", **auth_args_here)


Authentication with connection strings:
---------------------------------------

Username and password auth:

.. code-block:: python

    import fs
    dlkfs = fs.open_fs("dlk://username:password@/store_name/path/to/remote")


Tenant secret auth:

.. code-block:: python

    import fs
    dlkfs = fs.open_fs("dlk://username:password@tenant-id/store_name/path/to/remote")


Downloading files
-----------------

.. code-block:: python

    with open("local_file", "wb") as local_file:
        dlkfs.download("path/to/remote/file", local_file)


Uploading files
-----------------

.. code-block:: python

    with open("local_file", "wb") as local_file:
        dlkfs.upload("path/to/remote/file", local_file)
