class GXCloudIDAwareRef:
    """
    This class serves as a base class for refs tied to a Great Expectations Cloud ID.
    """

    def __init__(self, cloud_id: str) -> None:
        self._cloud_id = cloud_id

    @property
    def cloud_id(self):
        return self._cloud_id


class GXCloudResourceRef(GXCloudIDAwareRef):
    """
    This class represents a reference to a Great Expectations object persisted to Great Expectations Cloud.
    """

    def __init__(self, resource_type: str, cloud_id: str, url: str) -> None:
        self._resource_type = resource_type
        self._url = url
        super().__init__(cloud_id=cloud_id)

    @property
    def resource_type(self):
        # e.g. "checkpoint"
        return self._resource_type

    @property
    def url(self):
        return self._url
