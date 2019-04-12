from datetime import datetime
from unittest import TestCase

import yaml
import pytest

from six import text_type

from fs import open_fs
from fs import errors as fs_errors

SL = text_type("/")

class DLKFSIntegration(TestCase):
    @classmethod
    def setUpClass(cls):
        with open("config/test.yaml") as cfg_f:
            cls.cfg = yaml.safe_load(cfg_f).get("data-lake", {})

        if 'TEST_ROOT' not in cls.cfg:
            raise ValueError("TEST_ROOT should be defined in config/test.yaml")

        default_test_subdir = str(datetime.now()).replace(" ", "_").replace(".", "_").replace(":", "-")
        cls.cfg['TEST_SUBDIR'] = text_type(cls.cfg.get('TEST_SUBDIR', default_test_subdir))

        test_dlk = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/{TEST_ROOT}".format(**cls.cfg))
        test_dlk.makedir(cls.cfg['TEST_SUBDIR'])

    @classmethod
    def tearDownClass(cls):
        test_root_conn = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/{TEST_ROOT}/"
                                 .format(**cls.cfg))

        present_dirs = test_root_conn.listdir(cls.cfg['TEST_SUBDIR'])
        test_dirs = [name.replace("test_", "") + "/" for name in dir(cls) if name.startswith("test_")]
        utest_dirs = list(map(text_type, test_dirs))
        present_test_dirs = set(present_dirs).intersection(utest_dirs)

        for test_name in present_test_dirs:
            full_path = "/".join([cls.cfg['TEST_SUBDIR'], test_name])
            ufull_path = text_type(full_path)
            try:
                test_root_conn.removedir(ufull_path)
            except fs_errors.ResourceNotFound:
                pass

        test_root_conn.removedir(cls.cfg['TEST_SUBDIR'])

    @classmethod
    def _get_dlk(cls, run_dir=""):
        test_root_conn = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/{TEST_ROOT}/{TEST_SUBDIR}/"
                                 .format(**cls.cfg))
        if run_dir:
            urun_dir = text_type(run_dir)
            test_root_conn.makedir(urun_dir)
            fs_url_pat = "dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/{TEST_ROOT}/{TEST_SUBDIR}/{run_dir}"
            return open_fs(fs_url_pat.format(run_dir=run_dir, **cls.cfg))
        return test_root_conn

    def test_makedir_removedir(self):
        test_dlk = self._get_dlk("makedir_removedir")

        test_dir_name = text_type("subdir")
        test_dir_name_sl = text_type("subdir/")

        test_dlk.makedir(test_dir_name)
        has_subdir = test_dlk.listdir(SL)
        self.assertIn(test_dir_name_sl, has_subdir)

        with self.assertRaises(fs_errors.DirectoryExists):
            test_dlk.makedir(test_dir_name)

        test_dlk.makedir(test_dir_name, recreate=True)
        test_dlk.removedir(test_dir_name)

        no_subdir = test_dlk.listdir(SL)
        self.assertNotIn(test_dir_name_sl, no_subdir)

    def test_listdir(self):
        test_dlk = self._get_dlk("listdir")

        test_dir_name = text_type("subdir")
        test_dir_name_sl = text_type("subdir/")

        empty_ls = test_dlk.listdir(SL)
        self.assertIsInstance(empty_ls, list)

        test_dlk.makedir(test_dir_name, recreate=True)

        has_subdir = test_dlk.listdir(SL)
        self.assertIn(test_dir_name_sl, has_subdir)

        test_dlk.removedir(test_dir_name)

    def test_getinfo(self):
        test_dlk = self._get_dlk("getinfo")

        test_dir_name = text_type("subdir")
        test_dlk.makedir(test_dir_name, recreate=True)

        basic_info = test_dlk.getinfo(test_dir_name).raw
        self.assertIn("basic", basic_info)
        self.assertEqual(set(basic_info["basic"].keys()), set(map(text_type, ["name", "is_dir"])))

        details_info = test_dlk.getinfo(test_dir_name, namespaces=["details"]).raw
        self.assertIn("details", details_info)
        self.assertEqual(set(details_info["details"].keys()), set(map(text_type, ["type", "accessed", "modified", "size"])))

        access_info = test_dlk.getinfo(test_dir_name, namespaces=["access"]).raw
        self.assertIn("access", access_info)
        self.assertEqual(set(access_info["access"].keys()), set(map(text_type, ["owner", "group", "permission"])))

        _ = test_dlk.getinfo(test_dir_name, namespaces=["access"]).group

        dlk_info = test_dlk.getinfo(test_dir_name, namespaces=["dlk"]).raw
        self.assertIn("dlk", dlk_info)

        test_dlk.removedir(test_dir_name)

    def test_openbin_download_remove(self):
        test_dlk = self._get_dlk("openbin_download_remove")

        tmpdir = text_type("data")
        remote_fname = text_type("hello_world")
        remote_fname = text_type("hello_world")
        local_fname = "{tmpdir}/test.txt".format(tmpdir=tmpdir)

        verify_no_file = test_dlk.listdir(SL)
        self.assertNotIn(remote_fname, verify_no_file)

        with test_dlk.open(remote_fname, "wb") as f:
            f.write("Hello world!".encode())
        has_file = test_dlk.listdir(SL)
        self.assertIn(remote_fname, has_file)

        with open(local_fname, "wb") as f:
            test_dlk.download(remote_fname, f)
        with open(local_fname, "r") as f:
            self.assertEqual(f.read().strip(), "Hello world!")

        test_dlk.remove(remote_fname)
        verify_no_file = test_dlk.listdir(SL)
        self.assertNotIn(remote_fname, verify_no_file)

    def test_upload_remove(self):
        test_dlk = self._get_dlk("upload_remove")

        tmpdir = text_type("data")
        to_upload = text_type("upload.txt")

        verify_no_file = test_dlk.listdir(SL)
        self.assertNotIn(to_upload, verify_no_file)

        with open("data/upload.txt".format(tmpdir=tmpdir), "rb") as f:
            test_dlk.upload(to_upload, f)

        verify_file = test_dlk.listdir(SL)
        self.assertIn(to_upload, verify_file)

        test_dlk.remove(to_upload)
        verify_no_file = test_dlk.listdir(SL)
        self.assertNotIn(to_upload, verify_no_file)
