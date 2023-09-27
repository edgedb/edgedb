import urllib.parse
import httpx
import httpx_cache

class HttpClient(httpx.AsyncClient):
    def __init__(
        self, *args, edgedb_test_url: str | None, base_url: str, **kwargs
    ):
        self.edgedb_orig_base_url = None
        if edgedb_test_url:
            self.edgedb_orig_base_url = urllib.parse.quote(base_url, safe='')
            base_url = edgedb_test_url
        cache = httpx_cache.AsyncCacheControlTransport()
        super().__init__(*args, base_url=base_url, transport=cache, **kwargs)

    async def post(self, path, *args, **kwargs):
        if self.edgedb_orig_base_url:
            path = f'{self.edgedb_orig_base_url}/{path}'
        return await super().post(path, *args, **kwargs)

    async def get(self, path, *args, **kwargs):
        if self.edgedb_orig_base_url:
            path = f'{self.edgedb_orig_base_url}/{path}'
        return await super().get(path, *args, **kwargs)


