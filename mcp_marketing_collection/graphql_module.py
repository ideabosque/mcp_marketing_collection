#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Pytest configuration and fixtures for MCP RFQ Processor tests."""
from __future__ import annotations

__author__ = "bibow"

import httpx

from silvaengine_utility.graphql import Graphql


class GraphQLModule:
    """Encapsulates GraphQL module configuration and schema management.

    A module authenticates either with an AWS API Gateway ``x_api_key``
    (when ``x_api_key`` is provided) or with a silvaengine_gateway JWT
    Bearer token (when ``token_username``/``token_password`` — and optional
    ``gateway_base_url`` — are provided). When neither is configured the
    requests are sent unauthenticated.
    """

    def __init__(
        self,
        endpoint_id: str,
        module_name: str | None = None,
        class_name: str | None = None,
        endpoint: str | None = None,
        x_api_key: str | None = None,
        gateway_base_url: str | None = None,
        token_username: str | None = None,
        token_password: str | None = None,
        gateway_token: str | None = None,
    ):
        """
        Initialize GraphQL module configuration.

        Args:
            endpoint_id: Identifier for the endpoint
            module_name: Optional module name for schema generation
            class_name: Optional class name for schema generation
            endpoint: Optional endpoint URL template with {endpoint_id} placeholder
            x_api_key: Optional AWS API Gateway x-api-key value
            gateway_base_url: Optional silvaengine_gateway base URL used to
                obtain a JWT Bearer token via ``/auth/token``.
            token_username: Optional username for gateway JWT auth
            token_password: Optional password for gateway JWT auth
            gateway_token: Optional pre-issued JWT Bearer token. When provided
                it is reused as-is and the username/password login is skipped.
        """
        self._endpoint_id = endpoint_id
        self._module_name = module_name
        self._class_name = class_name
        self._endpoint = endpoint.format(endpoint_id=endpoint_id) if endpoint else None
        self._x_api_key = x_api_key
        self._gateway_base_url = gateway_base_url
        self._token_username = token_username
        self._token_password = token_password
        self._gateway_token = gateway_token
        self._schema = None

    @property
    def endpoint_id(self) -> str:
        """Get the endpoint identifier."""
        return self._endpoint_id

    @property
    def module_name(self) -> str | None:
        """Get the module name used for schema generation."""
        return self._module_name

    @property
    def class_name(self) -> str | None:
        """Get the class name used for schema generation."""
        return self._class_name

    @property
    def endpoint(self) -> str | None:
        """Get the formatted endpoint URL."""
        return self._endpoint

    @property
    def x_api_key(self) -> str | None:
        """Get the API key for authentication."""
        return self._x_api_key

    @property
    def gateway_base_url(self) -> str | None:
        """Get the silvaengine_gateway base URL (if any)."""
        return self._gateway_base_url

    @property
    def token_username(self) -> str | None:
        """Get the username used to obtain a gateway JWT."""
        return self._token_username

    @property
    def token_password(self) -> str | None:
        """Get the password used to obtain a gateway JWT."""
        return self._token_password

    @property
    def gateway_token(self) -> str | None:
        """Get a pre-issued gateway JWT Bearer token (if any)."""
        return self._gateway_token

    @property
    def auth_mode(self) -> str:
        """Determine which authentication mode this module uses.

        Returns one of:
            - ``"jwt"``: gateway JWT Bearer token (username/password or
              pre-issued ``gateway_token``)
            - ``"api_key"``: AWS API Gateway ``x-api-key`` header
            - ``"none"``: no authentication configured
        """
        if self._gateway_token or (
            self._token_username and self._token_password
        ):
            return "jwt"
        if self._x_api_key:
            return "api_key"
        return "none"

    @property
    def schema(self):
        """Get the cached GraphQL schema, loading it if necessary."""
        if self._schema is None and self._module_name and self._class_name:
            self.refresh_schema()
        return self._schema

    def refresh_schema(self):
        """Load or reload the GraphQL schema from the configured module and class."""
        if self._module_name and self._class_name:
            self._schema = Graphql.get_graphql_schema(
                module_name=self._module_name,
                class_name=self._class_name,
            )

    def get_gateway_token(self) -> str | None:
        """Obtain (or reuse) a JWT Bearer token for the silvaengine_gateway.

        Returns ``None`` when JWT auth is not configured for this module.
        A successfully issued token is cached on the instance and reused on
        subsequent calls.
        """
        if not self._gateway_token:
            if not (self._token_username and self._token_password):
                return None
            if not self._gateway_base_url:
                return None
            resp = httpx.post(
                f"{self._gateway_base_url.rstrip('/')}/auth/token",
                data={
                    "username": self._token_username,
                    "password": self._token_password,
                },
                timeout=15,
            )
            resp.raise_for_status()
            self._gateway_token = resp.json()["access_token"]
        return self._gateway_token