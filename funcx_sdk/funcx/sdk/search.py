from globus_sdk import SearchAPIError
from texttable import Texttable

from funcx.serialize import FuncXSerializer

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

    def __init__(self, client):
        """Initialize the Search Helper

        Parameters
        ----------
        authorizer : class:

        """
        self._sc = client

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
