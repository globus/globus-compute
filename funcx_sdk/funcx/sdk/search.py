from globus_sdk import SearchAPIError
from globus_sdk.search import SearchClient
from texttable import Texttable

from funcx.serialize import FuncXSerializer
from funcx.utils.errors import InvalidScopeException

SEARCH_SCOPE = "urn:globus:auth:scope:search.api.globus.org:all"

# Search limit defined by the globus API
SEARCH_LIMIT = 10000

# By default we will return 10 functions at a time
DEFAULT_SEARCH_LIMIT = 10


class SearchHelper:
    """Utility class for interacting with Globus search"""

    FUNCTION_SEARCH_INDEX_NAME = "funcx"
    FUNCTION_SEARCH_INDEX_ID = "673a4b58-3231-421d-9473-9df1b6fa3a9d"
    ENDPOINT_SEARCH_INDEX_NAME = "funcx_endpoints"
    ENDPOINT_SEARCH_INDEX_ID = "85bcc497-3ee9-4d73-afbb-2abf292e398b"

    def __init__(self, authorizer, owner_uuid):
        """Initialize the Search Helper

        Parameters
        ----------
        authorizer : class:

        """
        self._authorizer = authorizer
        self._owner_uuid = owner_uuid
        self._sc = SearchClient(authorizer=self._authorizer)

    def _exists(self, func_uuid):
        """

        Parameters
        ----------
        func_uuid

        Returns
        -------

        """
        try:
            res = self._sc.get_entry(SearchHelper.FUNCTION_SEARCH_INDEX_ID, func_uuid)
            return len(res.data["entries"]) > 0
        except SearchAPIError as err:
            if err.http_status == 404:
                return False
            raise err

    def search_function(self, q, offset=0, limit=DEFAULT_SEARCH_LIMIT, advanced=False):
        """Executes client side search.

        Parameters
        ----------
        q : str
            Free-form query input
        offset : int
            offset into total results
        limit : int
            max number of results to return
        advanced : bool
            enables advanced query syntax
        Returns
        -------
        FunctionSearchResults
        """
        response = self._sc.search(
            SearchHelper.FUNCTION_SEARCH_INDEX_ID,
            q,
            offset=offset,
            limit=limit,
            advanced=advanced,
        )

        # print(res)

        # Restructure results to look like the data dict in FuncXClient
        # see the JSON structure of res.data:
        #   https://docs.globus.org/api/search/search/#gsearchresult
        gmeta = response.data["gmeta"]
        results = []
        for item in gmeta:
            data = item["entries"][0]
            data["function_uuid"] = item["subject"]
            data = {**data, **data["content"]}
            del data["content"]
            results.append(data)

        return FunctionSearchResults(
            {
                "results": results,
                "offset": offset,
                "count": response.data["count"],
                "total": response.data["total"],
                "has_next_page": response.data["has_next_page"],
            }
        )

    def search_endpoint(self, q, scope="all", owner_id=None):
        """

        Parameters
        ----------
        q
        scope
        owner_id

        Returns
        -------

        """
        query = {"q": q, "filters": []}

        if owner_id:
            query["filters"].append(
                {"type": "match_all", "field_name": "owner", "values": [owner_id]}
            )

        scope_filter = None
        if scope == "my-endpoints":
            scope_filter = {
                "type": "match_all",
                "field_name": "owner",
                "values": [f"urn:globus:auth:identity:{self._owner_uuid}"],
            }
        elif scope == "shared-with-me":
            # TODO: filter for public=False AND owner != self._owner_uuid
            # need to build advanced query for that, because GFilters cannot do NOT
            # raise Exception('This scope has not been implemented')
            scope_filter = {
                "type": "match_all",
                "field_name": "public",
                "values": ["False"],
            }
        elif scope == "shared-by-me":
            # TODO: filter for owner=self._owner_uuid AND len(shared_with) > 0
            # but...how to filter for length of list...
            raise InvalidScopeException("This scope has not been implemented")
        elif scope != "all":
            raise InvalidScopeException("This scope is invalid")

        if scope_filter:
            query["filters"].append(scope_filter)

        print(query)
        resp = self._sc.post_search(self.ENDPOINT_SEARCH_INDEX_ID, query)
        gmeta = resp.data["gmeta"]
        results = []
        for res in gmeta:
            if (
                scope == "shared-with-me"
                and res["entries"][0]["content"]["owner"]
                == f"urn:globus:auth:identity:{self._owner_uuid}"
            ):
                continue
            data = res["entries"][0]
            data["endpoint_uuid"] = res["subject"]
            data = {**data, **data["content"]}
            del data["entry_id"]
            del data["content"]
            results.append(data)

        return results


class FunctionSearchResults(list):
    """Wrapper class to have better display of results"""

    FILTER_COLUMNS = {
        "function_code",
        "entry_id",
        "group",
        "public",
        "container_uuid",
        "function_source",
    }

    def __init__(self, gsearchresult):
        """

        Parameters
        ----------
        gsearchresult : dict
        """
        # wrapper for an array of results
        results = gsearchresult["results"]
        super().__init__(results)

        # track data about where we are in total results
        self.has_next_page = gsearchresult["has_next_page"]
        self.offset = gsearchresult["offset"]
        self.total = gsearchresult["total"]

        # we can use this to load functions and run them
        self.serializer = FuncXSerializer()

        # Reformat for pretty printing and easy viewing
        self._init_columns()
        self.table = Texttable(max_width=120)
        self.table.header(self.columns)
        for res in self:
            self.table.add_row([res[col] for col in self.columns])

    def _init_columns(self):
        self.columns = []
        if len(self):
            assert isinstance(self[0], dict)
            self.columns = [
                k
                for k in self[0].keys()
                if k not in FunctionSearchResults.FILTER_COLUMNS
            ]

    def __str__(self):
        if len(self):
            return self.table.draw()
        return "[]"

    def load_result(self, ix: int):
        """Get the code for a function.

        If in an ipython environment, this creates a new input and places the
        source in it.  Otherwise, the source code is printed.

        Parameters
        ----------
        ix : int
            index into the current list of results

        Returns
        -------
        None
        """
        res = self[ix]
        func_source = res["function_source"]
        # func = self.serializer.unpack_and_deserialize(packed_func)[0]
        # return func

        # if we also saved the source code of the function, we could interactively
        # generate a cell to edit the searched function
        # TODO: Strip the ipython bits and remove entirely
        print(func_source)
