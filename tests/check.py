import yaml
from fs import open_fs


def main():
    with open("config/test.yaml") as cfg_f:
        cfg = yaml.safe_load(cfg_f).get("data-lake", {})
    test_dlk = open_fs("dlk://{AZ_USERNAME}:{AZ_PASSWORD}@{AZ_TENANT_ID}/{STORE}/.".format(**cfg))
    print(test_dlk.listdir("."))


if __name__ == "__main__":
    main()
