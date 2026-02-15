#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Pytest configuration and fixtures for MCP RFQ Processor tests."""
from __future__ import annotations

__author__ = "bibow"

from silvaengine_utility.graphql import Graphql


class GraphQLModule:
    """Encapsulates GraphQL module configuration and schema management."""

    def __init__(
        self,
        endpoint_id: str,
        module_name: str | None = None,
        class_name: str | None = None,
        endpoint: str | None = None,
        x_api_key: str | None = None,
    ):
        """
        Initialize GraphQL module configuration.

        Args:
            endpoint_id: Identifier for the endpoint
            module_name: Optional module name for schema generation
            class_name: Optional class name for schema generation
            endpoint: Optional endpoint URL template with {endpoint_id} placeholder
        """
        self._endpoint_id = endpoint_id
        self._module_name = module_name
        self._class_name = class_name
        self._endpoint = endpoint.format(endpoint_id=endpoint_id) if endpoint else None
        self._x_api_key = x_api_key
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
