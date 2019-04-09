from pprint import pprint
import yaml
from fs import open_fs


with open("config/test.yaml") as cfg_f:
    TEST_CFG = yaml.safe_load(cfg_f).get("data-lake", {})
TEST_CFG['TEST_ROOT'] = 'test_sandbox'


def main():
    to_test = [
        test_listdir,
        test_makedir,
        test_getinfo,
        test_removedir,
        test_openbin
    ]

    for f in to_test:
        try:
            f()
        except Exception as e:
            print(f.__name__, "Failed", str(e))


def test_listdir():
    test_dlk = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/{TEST_ROOT}".format(**TEST_CFG))
    pprint(test_dlk.listdir("/"))
    pprint(test_dlk.listdir("subdir"))


def test_getinfo():
    test_dlk = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/test-sandbox".format(**TEST_CFG))
    pprint(test_dlk.getinfo("subdir").raw)
    pprint(test_dlk.getinfo("subdir", namespaces=["details"]).raw)
    pprint(test_dlk.getinfo("subdir", namespaces=["access"]).raw)
    pprint(test_dlk.getinfo("subdir", namespaces=["access"]).group)
    pprint(test_dlk.getinfo("subdir", namespaces=["dlk"]).raw)


def test_makedir():
    test_dlk = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/{TEST_ROOT}".format(**TEST_CFG))
    test_dlk.makedir("subdir")
    pprint(test_dlk.listdir("/"))
    test_dlk.makedir("subdir")


def test_removedir():
    test_dlk = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/{TEST_ROOT}".format(**TEST_CFG))
    test_dlk.makedir("subdir", recreate=True)
    pprint(test_dlk.listdir("/"))
    test_dlk.removedir("subdir")
    pprint(test_dlk.listdir("/"))


def test_openbin():
    test_dlk = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/{TEST_ROOT}".format(**TEST_CFG))
    with test_dlk.open("hello_world", "wb") as f:
        f.write("Hello world!".encode())
    pprint(test_dlk.listdir("/"))
    with open("test.txt", "wb") as f:
        test_dlk.download("hello_world", f)
    with open("test.txt", "r") as f:
        pprint(f.read())
    test_dlk.remove("hello_world")
    pprint(test_dlk.listdir("/"))


if __name__ == "__main__":
    main()
