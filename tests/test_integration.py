from datetime import datetime
from unittest import TestCase

import yaml
import pytest

from fs import open_fs
from fs import errors as fs_errors


class DLKFSIntegration(TestCase):
    @classmethod
    def setUpClass(cls):
        with open("config/test.yaml") as cfg_f:
            cls.cfg = yaml.safe_load(cfg_f).get("data-lake", {})

        if 'TEST_ROOT' not in cls.cfg:
            raise ValueError("TEST_ROOT should be defined in config/test.yaml")

        default_test_subdir = str(datetime.now()).replace(" ", "_").replace(".", "_").replace(":", "-")
        cls.cfg['TEST_SUBDIR'] = cls.cfg.get('TEST_SUBDIR', default_test_subdir)

        test_dlk = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/{TEST_ROOT}".format(**cls.cfg))
        test_dlk.makedir(cls.cfg['TEST_SUBDIR'])

    @classmethod
    def tearDownClass(cls):
        test_root_conn = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/{TEST_ROOT}/"
                                 .format(**cls.cfg))

        present_dirs = test_root_conn.listdir(cls.cfg['TEST_SUBDIR'])
        test_dirs = [name.replace("test_", "") + "/" for name in dir(cls) if name.startswith("test_")]
        present_test_dirs = set(present_dirs).intersection(test_dirs)

        for test_name in present_test_dirs:
            full_path = "/".join([cls.cfg['TEST_SUBDIR'], test_name])
            try:
                test_root_conn.removedir(full_path)
            except fs_errors.ResourceNotFound:
                pass

        test_root_conn.removedir(cls.cfg['TEST_SUBDIR'])

    @classmethod
    def _get_dlk(cls, run_dir=""):
        test_root_conn = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/{TEST_ROOT}/{TEST_SUBDIR}/"
                                 .format(**cls.cfg))
        if run_dir:
            test_root_conn.makedir(run_dir)
            fs_url_pat = "dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/{TEST_ROOT}/{TEST_SUBDIR}/{run_dir}"
            return open_fs(fs_url_pat.format(run_dir=run_dir, **cls.cfg))
        return test_root_conn

    def test_makedir_removedir(self):
        test_dlk = self._get_dlk("makedir_removedir")

        test_dlk.makedir("subdir")
        has_subdir = test_dlk.listdir("/")
        self.assertIn("subdir/", has_subdir)

        with self.assertRaises(fs_errors.DirectoryExists):
            test_dlk.makedir("subdir")

        test_dlk.makedir("subdir", recreate=True)
        test_dlk.removedir("subdir")

        no_subdir = test_dlk.listdir("/")
        self.assertNotIn("subdir/", no_subdir)

    def test_listdir(self):
        test_dlk = self._get_dlk("listdir")

        empty_ls = test_dlk.listdir("/")
        self.assertIsInstance(empty_ls, list)

        test_dlk.makedir("subdir", recreate=True)

        has_subdir = test_dlk.listdir("/")
        self.assertIn("subdir/", has_subdir)

        test_dlk.removedir("subdir")

    def test_getinfo(self):
        test_dlk = self._get_dlk("getinfo")

        test_dlk.makedir("subdir", recreate=True)

        basic_info = test_dlk.getinfo("subdir").raw
        self.assertIn("basic", basic_info)
        self.assertEqual(list(basic_info["basic"].keys()), ["name", "is_dir"])

        details_info = test_dlk.getinfo("subdir", namespaces=["details"]).raw
        self.assertIn("details", details_info)
        self.assertEqual(list(details_info["details"].keys()), ["type", "accessed", "modified", "size"])

        access_info = test_dlk.getinfo("subdir", namespaces=["access"]).raw
        self.assertIn("access", access_info)
        self.assertEqual(list(access_info["access"].keys()), ["owner", "group", "permission"])

        _ = test_dlk.getinfo("subdir", namespaces=["access"]).group

        dlk_info = test_dlk.getinfo("subdir", namespaces=["dlk"]).raw
        self.assertIn("dlk", dlk_info)

        test_dlk.removedir("subdir")

    def test_openbin_download_remove(self):
        tmpdir = "data"
        test_dlk = self._get_dlk("openbin_download_remove")

        verify_no_file = test_dlk.listdir("/")
        self.assertNotIn("hello_world", verify_no_file)

        with test_dlk.open("hello_world", "wb") as f:
            f.write("Hello world!".encode())
        has_file = test_dlk.listdir("/")
        self.assertIn("hello_world", has_file)

        with open("{tmpdir}/test.txt".format(tmpdir=tmpdir), "wb") as f:
            test_dlk.download("hello_world", f)
        with open("{tmpdir}/test.txt".format(tmpdir=tmpdir), "r") as f:
            self.assertEqual(f.read().strip(), "Hello world!")

        test_dlk.remove("hello_world")
        verify_no_file = test_dlk.listdir("/")
        self.assertNotIn("hello_world", verify_no_file)

    def test_upload_remove(self):
        tmpdir = "data"
        test_dlk = self._get_dlk("upload_remove")

        verify_no_file = test_dlk.listdir("/")
        self.assertNotIn("upload.txt", verify_no_file)

        with open("data/upload.txt".format(tmpdir=tmpdir), "rb") as f:
            test_dlk.upload("upload.txt", f)

        verify_file = test_dlk.listdir("/")
        self.assertIn("upload.txt", verify_file)

        test_dlk.remove("upload.txt")
        verify_no_file = test_dlk.listdir("/")
        self.assertNotIn("upload.txt", verify_no_file)
