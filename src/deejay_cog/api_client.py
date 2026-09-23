"""This cog's API client — and its identity.

deejay-cog authenticates to api-kaianolevine-com with its own named API key.
The key identifies the cog: the API matches it against the keys it holds in
configuration, so nothing here asserts a name and nothing has to be looked up
at runtime.

That is what makes the audit trail worth having. Every cog used to share one
Clerk machine secret, so the API could tell that *a* cog called it and never
which one — the per-caller attribution Project Keystone set out to get and did
not land. A key per cog is what makes the subject in the trail mean something,
and what makes one cog revocable without revoking the fleet.

The key proves who this cog is; it says nothing about what it may do. The API
decides that from its own declaration, and nothing sent from here can widen
it.

The shared client derives DEEJAY_COG_API_KEY from MACHINE_NAME, and the API
derives the same variable from the same name — one convention, no mapping to
keep in step. There is no fallback. The shared Clerk machine secret this replaced is gone:
every cog holding one credential indistinguishable from every other cog's was
the reason the API could tell that *a* cog called and never which one, so
keeping it available would have kept that ambiguity available.
"""

from __future__ import annotations

from mini_app_polis.api import KaianoApiClient

#: This cog's name in api-kaianolevine-com's identity_registry.MACHINES.
MACHINE_NAME = "deejay-cog"


def api_client(base_url: str | None = None) -> KaianoApiClient:
    """Build the API client with this cog's identity.

    Use this everywhere instead of ``KaianoApiClient(...)`` or
    ``KaianoApiClient.from_env()``, so the cog presents one identity from
    every call site rather than depending on which constructor was reached.
    """
    return KaianoApiClient(base_url=base_url, machine_name=MACHINE_NAME)
