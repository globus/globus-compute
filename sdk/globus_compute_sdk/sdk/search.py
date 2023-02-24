from globus_sdk import SearchAPIError


class SearchHelper:
    """Utility class for interacting with Globus search"""

    FUNCTION_SEARCH_INDEX_NAME = "funcx"
    FUNCTION_SEARCH_INDEX_ID = "673a4b58-3231-421d-9473-9df1b6fa3a9d"

    def __init__(self, client):
        self._sc = client

    def _exists(self, func_uuid):
        try:
            res = self._sc.get_entry(SearchHelper.FUNCTION_SEARCH_INDEX_ID, func_uuid)
            return len(res.data["entries"]) > 0
        except SearchAPIError as err:
            if err.http_status == 404:
                return False
            raise err

    def search_function(self, q, **kwargs):
        """Executes client side search."""
        return self._sc.search(SearchHelper.FUNCTION_SEARCH_INDEX_ID, q, **kwargs)
