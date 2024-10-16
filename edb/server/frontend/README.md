## Authentication and Transports:

| Auth Method | Description | Available Transports | Notes |
|-------------|-------------|----------------------|-------|
| Trust | Always-trust policy, disables all authentication | All | [1] |
| SCRAM | Password-based authentication using challenge-response scheme | TCP, TCP_PG | Default for TCP and TCP_PG |
| JWT | Uses a JWT signed by the server to authenticate | TCP, TCP_PG, HTTP, WEBSOCKET, SIMPLE_HTTP | [4] [5] |
| Password | Simple password-based authentication over TLS | SIMPLE_HTTP | [2] |
| mTLS | Mutual TLS authentication | All | [3] [6] |

| Transport        | Description                                       | Available Auth Methods                        |
|------------------|---------------------------------------------------|-----------------------------------------------|
| TCP              | EdgeDB binary protocol                            | SCRAM (default), JWT[4], Trust[1], mTLS[6]    |
| TCP_PG           | Postgres protocol for SQL query mode              | SCRAM (default), JWT[4], Trust[1], mTLS[6]    |
| HTTP             | EdgeDB binary protocol tunneled over HTTP         | JWT[5] (default), Trust[1], mTLS[6]           |
| WEBSOCKET        | EdgeDB/Postgres over WebSocket                    | JWT[5] (default), Trust[1], mTLS[6]           |
| SIMPLE_HTTP      | EdgeQL over HTTP, Notebook and GraphQL endpoints  | Password[2] (default), JWT[5], Trust[1], mTLS |
| HTTP_METRICS [7] | Metrics endpoint                                  | Trust[1] (default), mTLS[3]                   |
| HTTP_HEALTH  [7] | Health check endpoint                             | Trust[1] (default), mTLS[3]                   |

Notes:
 - [1] Trust can be configured for any transport
 - [2] Password is only available for SIMPLE_HTTP, where it is the default.
   Passwords are provided using HTTP Basic auth and internally handled via
   SCRAM.
 - [3] Auto for HTTP_METRICS and HTTP_HEALTH will use mTLS if TLS client CA file
   is provided, otherwise Trust
 - [4] JWT for TCP/TCP_PG requires the token to be passed in `secret_key`
   startup parameter
 - [5] JWT for HTTP requires `Authorization: bearer ...`
 - [6] mTLS for all transports other than SIMPLE_HTTP requires out-of-band
   username
 - [7] HTTP_METRICS and HTTP_HEALTH are configured entirely outside the database
   and use the default auth methods

## Database tenancy structure:

- Tenant ID: determined by SNI
- Database (legacy)
- Branch

Multi-tenancy requires SSL. Tenant IDs extracted from the SSL SNI information for all transports.

## HTTP CORS (Cross-Origin Resource Sharing)

CORS headers set by EdgeDB:

- `Access-Control-Allow-Origin`: Specifies which origins are allowed to make cross-origin requests. Set to the requesting origin if it's allowed, or omitted if not.
- `Access-Control-Expose-Headers`: Lists headers that browsers are allowed to access. Only set if specific headers are configured to be exposed.
- `Access-Control-Allow-Methods`: Specifies the HTTP methods allowed when accessing the resource. Typically set to "GET, POST, OPTIONS" for EdgeDB endpoints.
- `Access-Control-Allow-Headers`: Indicates which HTTP headers can be used during the actual request. Usually includes headers like "Authorization", "Content-Type", etc.
- `Access-Control-Allow-Credentials`: When set to "true", indicates that the actual request can include user credentials (like cookies, HTTP authentication, or client-side SSL certificates).
- `Access-Control-Max-Age`: Specifies how long the results of a preflight request can be cached. Can be used to optimize performance.

- Checks for CORS configuration in the database or system config.
- Validates the request origin against allowed origins.
- For valid origins, sets appropriate CORS headers:
  - `Access-Control-Allow-Origin`
  - `Access-Control-Allow-Headers`
      - `Authorization` for auth
      - `Authorization`, `X-EdgeDB-User` for all paths
  - `Access-Control-Expose-Headers`
      - `WWW-Authenticate`, `Authentication-Info` for auth
      - `EdgeDB-Protocol-Version` for notebook
- For OPTIONS requests:
  - Sets status to 204 (No Content)
  - Adds headers:
    - `Access-Control-Allow-Methods`
    - `Access-Control-Allow-Headers`
    - `Access-Control-Allow-Credentials` for extension paths

## X-Forwarded-

`X-Forwarded-` headers are allowed for `schema`, `port` and `host`.

## HTTP Endpoints

HTTP Endpoints handled internally:

- /auth/token
   - Method: GET
   - Description: Handle authentication
   - Request:
     - Header: Authorization: {AUTH METHOD} data={PAYLOAD}
   - Response:
     - Header: www-authenticate: {AUTH METHOD} {AUTH PAYLOAD}
     - Body: Authorization token (on successful authentication)

- /auth/*
   - Description: Additional auth extension paths

- /{branch,db}/{BRANCH}
   - Method: POST
   - Description: Execute EdgeQL queries
   - Request:
     - Header: X-EdgeDB-User: {USERNAME}
     - Header: Authorization: Bearer {TOKEN}
     - Header: Content-Type: application/x.edgedb.v_1_0.binary
   - Response:
     - Content-Type: application/x.edgedb.v_1_0.binary
     - Body: Message format as described in the protocol

- /{branch,db}/{BRANCH}/edgeql
   - Method: POST
   - Description: Execute EdgeQL queries over HTTP

- /{branch,db}/{BRANCH}/notebook
   - Method: POST
   - Description: Serve EdgeDB notebook protocol. This protocol does not allow for transaction commits or DDL.

Additional HTTP endpoints:

- /{branch,db}/{BRANCH}/graphql
   - Method: POST
   - Description: Execute GraphQL queries

- /{branch,db}/{BRANCH}/ext/auth
   - Method: GET, POST
   - Description: Handle authentication extension requests

- /{branch,db}/{BRANCH}/ai
   - Method: POST
   - Description: Handle AI-related requests

- /metrics
   - Method: GET
   - Description: Expose metrics in OpenMetrics text format

- /server-info
   - Method: GET
   - Description: Provide server information (only available in development or test mode)

- /server/status/{ready,alive}
   - Method: GET
   - Description: Handles system API requests

- /ui
   - Method: GET
   - Description: Serve the admin UI (if enabled)

- /ui/_static
   - Description: Static UI files
