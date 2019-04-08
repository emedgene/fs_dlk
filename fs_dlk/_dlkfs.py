from fs.osfs import OSFS
from fs.base import FS

class DLKFS(OSFS):
    def __init__(
            self,
            dir_path="/",
            az_username=None,
            az_password=None,
            az_tenant_id=None,
            store=None
    ):
        return super().__init__(dir_path)
