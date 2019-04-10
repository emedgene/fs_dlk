# coding: utf-8
"""Defines the DLKFSOpener."""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

__all__ = ['DLKFSOpener']

from fs.opener import Opener

from ._dlkfs import DLKFS


class DLKFSOpener(Opener):
    protocols = ['dlk']

    def open_fs(self, fs_url, parse_result, writeable, create, cwd):
        resource = parse_result.resource.split("/")
        tenant_id = resource[0]
        store = resource[1]
        dir_path = "/".join(resource[2:])

        if tenant_id:
            auth_args = {
                "tenant_id": tenant_id,
                "client_id": parse_result.username or None,
                "client_secret": parse_result.password or None,
            }
        else:
            auth_args = {
                "username": parse_result.username or None,
                "password": parse_result.password or None,
            }

        dlkfs = DLKFS(
            dir_path=dir_path or '/',
            store=store or None,
            **auth_args
        )

        return dlkfs
