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
    ]

    for f in to_test:
        try:
            f()
        except Exception as e:
            print(f.__name__, "Failed", str(e))


def test_listdir():
    test_dlk = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/{TEST_ROOT}".format(**TEST_CFG))
    print(test_dlk.listdir("."))
    print(test_dlk.listdir("subdir"))


def test_getinfo():
    test_dlk = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/test-sandbox".format(**TEST_CFG))
    print(test_dlk.getinfo("subdir").raw)
    print(test_dlk.getinfo("subdir", namespaces=["details"]).raw)
    print(test_dlk.getinfo("subdir", namespaces=["access"]).raw)
    print(test_dlk.getinfo("subdir", namespaces=["access"]).group)
    print(test_dlk.getinfo("subdir", namespaces=["dlk"]).raw)


def test_makedir():
    test_dlk = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/{TEST_ROOT}".format(**TEST_CFG))
    test_dlk.makedir("subdir")
    print(test_dlk.listdir("/"))
    test_dlk.makedir("subdir")


if __name__ == "__main__":
    main()
