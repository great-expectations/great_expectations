from __future__ import annotations


class GXCloudIDAwareRef:
    """
    This class serves as a base class for refs tied to a Great Expectations Cloud ID.
    """

    def __init__(self, id: str) -> None:
        self._id = id

    @property
    def id(self):
        return self._id


class GXCloudResourceRef(GXCloudIDAwareRef):
    """
    This class represents a reference to a Great Expectations object persisted to Great Expectations Cloud.
    """

    def __init__(
        self, resource_type: str, id: str, url: str, response_json: dict
    ) -> None:
        self._resource_type = resource_type
        self._url = url
        self._response = response_json
        super().__init__(id=id)

    @property
    def resource_type(self):
        # e.g. "checkpoint"
        return self._resource_type

    @property
    def url(self):
        return self._url

    @property
    def response(self):
        return self._response
