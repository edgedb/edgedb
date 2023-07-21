import httpx
import urllib.parse

from . import base


class GitHubProvider(base.BaseProvider):
    def __init__(self, *args, **kwargs):
        super().__init__("github", *args, **kwargs)

    def get_code_url(
        self, state: str, redirect_uri: str, scope: str = "read:user"
    ) -> str:
        params = {
            "client_id": self.client_id,
            "scope": scope,
            "state": state,
            "redirect_uri": redirect_uri,
        }
        encoded = urllib.parse.urlencode(params)
        return f"https://github.com/login/oauth/authorize?{encoded}"

    async def exchange_access_token(
        self, code: str, state: str, redirect_uri: str
    ):
        # Check state value
        # TODO: Look up state value from FlowState object
        # flow_state = await db.get_flow_state(state, "github")
        # if flow_state is None:
        #    raise errors.UnauthorizedError("invalid state value")
        data = {
            'grant_type': 'authorization_code',
            'code': code,
            'state': state,
            "redirect_uri": redirect_uri,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
        }

        headers = {'Content-Type': 'application/json'}

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://github.com/login/oauth/access_token",
                json=data,
                headers=headers,
            )
            token = resp.json()['access_token']

            return token
