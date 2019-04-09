import yaml
from fs import open_fs


with open("config/test.yaml") as cfg_f:
    TEST_CFG = yaml.safe_load(cfg_f).get("data-lake", {})


def main():
    to_test = [
        test_listdir,
        test_getinfo
    ]

    for f in to_test:
        try:
            f()
        except Exception as e:
            print(f.__name__, "Failed", str(e))


def test_listdir():
    test_dlk = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/".format(**TEST_CFG))
    print(test_dlk.listdir("."))
    print(test_dlk.listdir("emg-cases/test_b2702/EMG184980315/Case2Pvcf/v1.0.6_a7d61de/"))
    print(test_dlk.listdir("emg-cases/test_b2702/EMG184980315/Case2Pvcf/v1.0.6_a7d61de"))
    print(test_dlk.listdir("emg-cases/test_b2702/EMG184980315/Case2Pvcf/v1.0.6_a7d61de/EMG184980315.vcf.gz"))
    print(test_dlk.listdir("aaa"))


def test_getinfo():
    test_dlk = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/".format(**TEST_CFG))
    print(test_dlk.getinfo("emg-cases").raw)
    print(test_dlk.getinfo("emg-cases", namespaces=["details"]).raw)
    print(test_dlk.getinfo("emg-cases", namespaces=["access"]).raw)
    print(test_dlk.getinfo("emg-cases", namespaces=["access"]).group)
    print(test_dlk.getinfo("emg-cases", namespaces=["dlk"]).raw)


if __name__ == "__main__":
    main()
