class BaseProvider:
    def __init__(self, name: str, client_id: str, client_secret: str):
        self.name = name
        self.client_id = client_id
        self.client_secret = client_secret

    def get_code_url(self) -> str:
        raise NotImplementedError

    async def exchange_code(self, code: str) -> str:
        raise NotImplementedError

    async def fetch_user_info(self, token: str) -> dict:
        raise NotImplementedError
