# coding: utf-8
"""Defines the DLKFSOpener."""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

__all__ = ['DLKFSOpener']

from fs.opener import Opener
from fs.opener.errors import OpenerError

from ._dlkfs import DLKFS


class DLKFSOpener(Opener):
    protocols = ['dlk']

    def open_fs(self, fs_url, parse_result, writeable, create, cwd):
        resource = parse_result.resource.split("/")
        tenant_id = resource[0]
        store = resource[1]
        dir_path = "/".join(resource[2:])
        dlkfs = DLKFS(
            dir_path=dir_path or '/',
            client_id=parse_result.username or None,
            client_secret=parse_result.password or None,
            tenant_id=tenant_id or None,
            store=store or None,
        )

        return dlkfs
