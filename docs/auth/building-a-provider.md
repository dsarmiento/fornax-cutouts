---
title: Building an Auth Provider
description: Step-by-step tutorial for implementing and registering an auth provider
---

# Building an Auth Provider

This guide walks through implementing an auth provider and registering it with the Fornax Cutouts auth registry, to raise or remove the anonymous cutout limit for known users.

---

## What Is an Auth Provider?

An auth provider is a Python class that answers one question: *given a request, who is making it, and what cutout budget should they get?*

The framework calls your provider's `resolve()` method before dispatching an async job. Everything else — reserving the budget, checking it against the rolling window, reconciling it against the actual cutout count — is handled by Fornax Cutouts.

---

## Step 0: Enable cutout limiting

Limits are disabled until you opt in:

```bash
CUTOUTS__CUTOUT_LIMIT__ENABLED=true
CUTOUTS__CUTOUT_LIMIT__ANON_CUTOUT_LIMIT=10
CUTOUTS__CUTOUT_LIMIT__WINDOW_SECONDS=60
```

If you run behind a load balancer or ingress, also set `CUTOUTS__NUM_TRUSTED_PROXIES` so anonymous identities use the real client IP (see [Auth Providers Overview](overview.md#anonymous-principals)).

---

## Step 1: Create the Provider File

Create a new `.py` file anywhere under your `CUTOUTS__SOURCE_PATH` directory — the same directory used for mission sources:

```
sources/
├── my_mission.py
└── my_auth_provider.py
```

The file name does not matter. The registry discovers all `*.py` files under the configured path recursively, for both mission sources and auth providers.

To keep the provider outside the mission sources directory, point `CUTOUTS__CUTOUT_LIMIT__PRINCIPAL_RESOLVER` at the file (or at a directory to glob) instead; it is loaded at startup along with the mission sources.

```bash
CUTOUTS__CUTOUT_LIMIT__PRINCIPAL_RESOLVER=/app/auth/my_auth_provider.py
```

---

## Step 2: Implement the Provider Class

```python
from starlette.requests import Request

from fornax_cutouts.auth import AbstractAuthProvider, Principal, auth_registry


@auth_registry.register_provider
class ApiKeyAuthProvider(AbstractAuthProvider):
    name = "api_key"

    # Replace with a real lookup (database, cache, secrets manager, etc.)
    _KNOWN_KEYS = {
        "trusted-partner-key": Principal(identity="trusted-partner", cutout_limit=50_000),
        "internal-key": Principal(identity="internal", cutout_limit=None),  # unlimited
    }

    async def resolve(self, request: Request) -> Principal | None:
        api_key = request.headers.get("x-api-key")
        if api_key is None:
            return None  # Not our concern; fall back to the anonymous default

        return self._KNOWN_KEYS.get(api_key)
```

### Key points

- The `@auth_registry.register_provider` decorator registers a single provider instance under its `name` attribute. Registering a second provider raises `RuntimeError`.
- `resolve()` is `async` and receives the raw Starlette `Request`, so you can read headers, cookies, or query params.
- Return `None` when the request doesn't carry credentials this provider understands — this lets the anonymous fallback handle it.
- Raise `PrincipalResolutionError` when the request is clearly for your provider but resolution fails (backend down, misconfiguration). The registry logs and falls back to the anonymous limit. Do not use this for "wrong password" style cases where anonymous fallback is acceptable — return `None` instead.
- Any other exception is not caught and will surface as HTTP 500.
- `identity` should be a stable, opaque string — a hashed user ID or API key ID, not raw PII.

---

## Step 3: Choose a Budget

`Principal.cutout_limit` fully controls the cutout limit for that identity:

```python
Principal(identity="trusted-partner", cutout_limit=50_000)  # raise the limit
Principal(identity="internal", cutout_limit=None)            # remove the limit entirely
```

Optionally override the rolling window per identity too:

```python
Principal(identity="burst-tier", cutout_limit=200, window_seconds=60)  # 200 cutouts per minute
```

Leaving `window_seconds=None` (the default) uses `CUTOUTS__CUTOUT_LIMIT__WINDOW_SECONDS`.

---

## Step 4: Handle Real Auth Backends

For a real identity provider (JWT, OAuth introspection, session lookup), do the verification inside `resolve()` and return `None` on failure so requests without valid credentials fall back to the anonymous limit instead of erroring:

```python
import jwt
from starlette.requests import Request

from fornax_cutouts.auth import AbstractAuthProvider, Principal, auth_registry


@auth_registry.register_provider
class JwtAuthProvider(AbstractAuthProvider):
    name = "jwt"

    async def resolve(self, request: Request) -> Principal | None:
        auth_header = request.headers.get("authorization", "")
        if not auth_header.startswith("Bearer "):
            return None

        token = auth_header.removeprefix("Bearer ")
        try:
            claims = jwt.decode(token, options={"verify_signature": True}, algorithms=["RS256"])
        except jwt.PyJWTError:
            return None  # Invalid token → anonymous limit, same as missing credentials

        return Principal(
            identity=claims["sub"],
            cutout_limit=claims.get("cutout_limit", 5_000),
        )
```

For failures that mean your auth backend is broken rather than the caller presenting bad credentials, raise `PrincipalResolutionError`:

```python
from fornax_cutouts.utils.exceptions import PrincipalResolutionError

raise PrincipalResolutionError(self.name, "user directory unreachable")
```

---

## Step 5: Verify Registration

Start the API and check the startup logs:

```bash
fornax-cutouts api
```

At **info** level you should see registration when the module loads:

```
Registered api_key as the authz provider
```

At **debug** level, after `discover_sources()` completes:

```
Registered mission sources: ['my_mission']; auth provider: api_key
```

---

## Step 6: Test the Limit

With cutout limiting enabled:

```bash
# Anonymous request — subject to CUTOUTS__CUTOUT_LIMIT__ANON_CUTOUT_LIMIT
curl -i -X POST http://localhost:8000/api/v0/cutouts/async \
  -F "position[]=83.8221,-5.3911" \
  -F "size=500"

# Authenticated request — uses the provider's Principal.cutout_limit
curl -i -X POST http://localhost:8000/api/v0/cutouts/async \
  -H "X-API-Key: trusted-partner-key" \
  -F "position[]=83.8221,-5.3911" \
  -F "size=500"
```

Once an identity's budget is exhausted within the window, subsequent requests receive `429 Too Many Requests` with a `Retry-After` header.

If a job passes the initial reservation but the worker discovers more cutouts than expected (e.g. multiple filters per position), reconcile may fail the job with `ExecutionPhase.ERROR` instead. The client should check the job's error summary and retry after the `Retry-After` window.

---

## Step 7: Run the test suite

Auth limiter logic is covered under `tests/auth/` (Lua scripts and `CutoutLimiter` integration). CI runs these against a Valkey service; locally:

```bash
# Valkey or Redis on localhost:6379
uv sync --group test
uv run pytest tests/auth/ -v
```

---

## Checklist

- [ ] `CUTOUTS__CUTOUT_LIMIT__ENABLED=true` in environments where limits should apply
- [ ] `CUTOUTS__NUM_TRUSTED_PROXIES` set correctly when behind reverse proxies
- [ ] Provider file is under `CUTOUTS__SOURCE_PATH` or at `CUTOUTS__CUTOUT_LIMIT__PRINCIPAL_RESOLVER`
- [ ] Class is decorated with `@auth_registry.register_provider`
- [ ] `resolve()` returns `None` for requests it doesn't recognize
- [ ] `PrincipalResolutionError` used only for provider/backend failures that should degrade to anonymous
- [ ] `identity` is a stable, opaque string (no raw PII)
- [ ] `cutout_limit` reflects the intended budget (`None` for unlimited)
- [ ] Provider name appears in startup logs
