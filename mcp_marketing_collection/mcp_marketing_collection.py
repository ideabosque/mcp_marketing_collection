#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

__author__ = "bibow"

import logging
import traceback
from typing import Any, Dict

import httpx
import humps
from silvaengine_dynamodb_base.models import GraphqlSchemaModel
from silvaengine_utility.graphql import Graphql
from silvaengine_utility.serializer import Serializer

from .graphql_module import GraphQLModule

MCP_CONFIGURATION = {
    "tools": [
        {
            "name": "get_place",
            "description": "Retrieves an existing place by UUID or finds/creates a place based on location data (region, latitude, longitude, address). When providing location data along with business details (business_name, phone_number, website, types), it will create a new place if none exists at that location, or update the existing place if the details have changed. Returns the complete place object with place_uuid.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "place_uuid": {
                        "type": "string",
                        "description": "UUID of the place to fetch (alternative to location-based lookup)",
                    },
                    "region": {"type": "string", "description": "Region identifier"},
                    "latitude": {
                        "type": "number",
                        "description": "Latitude coordinate",
                    },
                    "longitude": {
                        "type": "number",
                        "description": "Longitude coordinate",
                    },
                    "address": {"type": "string", "description": "Full address"},
                    "business_name": {
                        "type": "string",
                        "description": "Name of the business",
                    },
                    "phone_number": {"type": "string", "description": "Phone number"},
                    "website": {"type": "string", "description": "Website URL"},
                    "types": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Array of business types",
                    },
                },
                "required": [],
            },
            "annotations": None,
        },
        {
            "name": "get_contact_profile",
            "description": "Retrieves an existing contact profile by email or creates a new one with contact information (email, first_name, last_name). Optionally associates the contact with a place using place_uuid. If the contact exists but details (name or place association) have changed, it updates the profile. Returns the complete contact profile including contact_uuid and associated place information.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "contact": {
                        "type": "object",
                        "description": "Contact information",
                        "properties": {
                            "email": {"type": "string"},
                            "first_name": {"type": "string"},
                            "last_name": {"type": "string"},
                        },
                        "required": ["email"],
                    },
                    "place": {
                        "type": "object",
                        "description": "Place information",
                        "properties": {"place_uuid": {"type": "string"}},
                        "required": ["place_uuid"],
                    },
                },
                "required": ["contact"],
            },
            "annotations": None,
        },
        {
            "name": "data_collect",
            "description": "Processes and stores collected marketing data for a contact. Accepts a JSON dataset containing place_uuid, contact information (email, first_name, last_name), and additional custom data fields (all fields will be stored). Creates or updates the contact profile with the provided data, automatically assigns the configured sales representative, and returns the contact_uuid along with sales rep contact information for follow-up.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "data_collect_dataset": {
                        "type": "string",
                        "description": "JSON string containing collected data including place_uuid, email, first_name, last_name and other fields",
                    }
                },
                "required": ["data_collect_dataset"],
            },
            "annotations": None,
        },
        {
            "name": "submit_request",
            "description": "Creates a new contact request record in the system with a title and detailed description. Links the request to both a place (via place_uuid) and a contact (via contact_uuid), enabling tracking of customer inquiries, support requests, or sales opportunities. Returns the generated request_uuid for reference and follow-up tracking.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "place_uuid": {
                        "type": "string",
                        "description": "UUID of the place",
                    },
                    "contact_uuid": {
                        "type": "string",
                        "description": "UUID of the contact",
                    },
                    "request_title": {
                        "type": "string",
                        "description": "Title of the request",
                    },
                    "request_detail": {
                        "type": "string",
                        "description": "Detailed description of the request",
                    },
                },
                "required": [
                    "place_uuid",
                    "contact_uuid",
                    "request_title",
                    "request_detail",
                ],
            },
            "annotations": None,
        },
    ],
    "resources": [],
    "prompts": [],
    "module_links": [
        {
            "type": "tool",
            "name": "get_place",
            "module_name": "mcp_marketing_collection",
            "class_name": "MCPMarketingCollection",
            "function_name": "get_place",
            "return_type": "text",
        },
        {
            "type": "tool",
            "name": "get_contact_profile",
            "module_name": "mcp_marketing_collection",
            "class_name": "MCPMarketingCollection",
            "function_name": "get_contact_profile",
            "return_type": "text",
        },
        {
            "type": "tool",
            "name": "data_collect",
            "module_name": "mcp_marketing_collection",
            "class_name": "MCPMarketingCollection",
            "function_name": "data_collect",
            "return_type": "text",
        },
        {
            "type": "tool",
            "name": "submit_request",
            "module_name": "mcp_marketing_collection",
            "class_name": "MCPMarketingCollection",
            "function_name": "submit_request",
            "return_type": "text",
        },
    ],
    "modules": [
        {
            "package_name": "mcp_marketing_collection",
            "module_name": "mcp_marketing_collection",
            "class_name": "MCPMarketingCollection",
            "setting": {
                "keyword": "marketing",
                "google_api_key": "<google_api_key>",
                "sales_rep": "Marketing Team",
                "sales_rep_email": "marketing@company.com",
                "shopify_endpoint_id": "shopify_store",
                "promotion_products": [],
            },
        }
    ],
}


class MCPMarketingCollection:
    def __init__(self, logger: logging.Logger, **setting: Dict[str, Any]):
        self.logger = logger
        self.setting = setting
        self._endpoint_id = None
        self._part_id = None
        self._graphql_modules = {}

    @property
    def endpoint_id(self) -> str | None:
        return self._endpoint_id

    @endpoint_id.setter
    def endpoint_id(self, value: str):
        self._endpoint_id = value

    @property
    def part_id(self) -> str | None:
        return self._part_id

    @part_id.setter
    def part_id(self, value: str):
        self._part_id = value

    def get_graphql_module(self, module_name: str) -> GraphQLModule | None:
        """Get a GraphQL module by name."""
        if not self._graphql_modules.get(module_name):
            self._graphql_modules[module_name] = GraphQLModule(
                endpoint_id=self.endpoint_id,
                module_name=module_name,
                class_name=self.setting.get("graphql_modules", {})
                .get(module_name, {})
                .get("class_name"),
                endpoint=self.setting.get("graphql_modules", {})
                .get(module_name, {})
                .get("endpoint"),
                x_api_key=self.setting.get("graphql_modules", {})
                .get(module_name, {})
                .get("x_api_key"),
            )

        return self._graphql_modules.get(module_name)

    def _execute_graphql_query(
        self,
        function_name: str,
        operation_name: str,
        operation_type: str,
        variables: Dict[str, Any],
        module_name: str = "ai_marketing_engine",
    ) -> Dict[str, Any]:
        try:
            graphql_module = self.get_graphql_module(module_name)
            query = GraphqlSchemaModel.get_schema(
                endpoint_id=graphql_module.endpoint_id,
                operation_type=operation_type,
                operation_name=operation_name,
                module_name=module_name,
                enable_preferred_custom_schema=True,
            )

            if not query:
                query = Graphql.generate_graphql_operation(
                    operation_name, operation_type, graphql_module.schema
                )

            payload = Serializer.json_dumps({"query": query, "variables": variables})

            headers = {
                "x-api-key": graphql_module.x_api_key,
                "Part-Id": self.part_id,
                "Content-Type": "application/json",
            }

            with httpx.Client(http2=True, timeout=httpx.Timeout(30.0)) as client:
                response = client.post(
                    graphql_module.endpoint,
                    headers=headers,
                    content=payload,
                )

            result = response.json()

            if "errors" in result:
                error_message = result["errors"][0].get("message", "GraphQL error")
                raise Exception(f"GraphQL error: {error_message}")

            return result.get("data", {}).get(operation_name)
        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise Exception(
                f"Failed to execute GraphQL query ({function_name}/{self.endpoint_id}). Error: {e}"
            )

    # * MCP Function.
    def get_place(self, **arguments: Dict[str, Any]) -> Dict[str, Any]:
        """ """
        try:
            self.logger.info(f"Arguments: {arguments}")

            if arguments.get("place_uuid"):
                place = self._execute_graphql_query(
                    "ai_marketing_graphql",
                    "place",
                    "Query",
                    {"placeUuid": arguments["place_uuid"]},
                )

                return humps.decamelize(place)

            assert all(
                arguments.get(k) for k in ["region", "latitude", "longitude", "address"]
            ), "Missing required arguments"

            variables = {
                "region": arguments["region"],
                "latitude": arguments["latitude"],
                "longitude": arguments["longitude"],
                "address": arguments["address"],
            }
            result = self._execute_graphql_query(
                "ai_marketing_graphql",
                "placeList",
                "Query",
                variables,
            )

            if result["total"] > 0:
                place = humps.decamelize(result["placeList"][0])
                variables.update({"placeUuid": place["place_uuid"]})

                if all(
                    [
                        place.get("business_name") == arguments.get("business_name"),
                        place.get("phone_number") == arguments.get("phone_number"),
                        place.get("website") == arguments.get("website"),
                        sorted(place.get("types", []))
                        == sorted(arguments.get("types", [])),
                    ]
                ):
                    return place

            variables.update(
                {
                    "businessName": arguments.get("business_name"),
                    "phoneNumber": arguments.get("phone_number"),
                    "website": arguments.get("website"),
                    "types": arguments.get("types", []),
                    "updatedBy": "Admin",
                }
            )
            result = self._execute_graphql_query(
                "ai_marketing_graphql",
                "insertUpdatePlace",
                "Mutation",
                variables,
            )
            place = humps.decamelize(result["place"])

            return place

        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise e

    # * MCP Function.
    def get_contact_profile(self, **arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Get or create contact profile."""
        try:
            self.logger.info(f"Arguments: {arguments}")
            contact = arguments["contact"]
            place = arguments.get("place", {})
            variables = {
                "email": contact["email"],
            }
            result = self._execute_graphql_query(
                "ai_marketing_graphql",
                "contactProfileList",
                "Query",
                variables,
            )

            if result["total"] > 0:
                contact_profile = humps.decamelize(result["contactProfileList"][0])
                variables.update({"contactUuid": contact_profile["contact_uuid"]})

                if all(
                    [
                        contact_profile.get("first_name") == contact.get("first_name"),
                        contact_profile.get("last_name") == contact.get("last_name"),
                        contact_profile["place"].get("place_uuid")
                        == place.get("place_uuid"),
                    ]
                ):
                    return contact_profile

            variables.update(
                {
                    "firstName": contact.get("first_name"),
                    "lastName": contact.get("last_name"),
                    "placeUuid": place.get("place_uuid"),
                    "updatedBy": "Admin",
                }
            )
            # Remove None or empty string values to avoid DynamoDB validation errors
            # Special handling for contactUuid - completely remove it if not valid
            self.logger.info(f"Variables before filtering: {variables}")
            mutation_variables = {}
            for k, v in variables.items():
                # Skip None, empty strings, or empty lists
                if v in (None, "", []):
                    continue
                # Special check for contactUuid - must be non-empty after stripping
                if k == "contactUuid" and (not v or not str(v).strip()):
                    continue
                mutation_variables[k] = v
            self.logger.info(f"Variables after filtering: {mutation_variables}")
            result = self._execute_graphql_query(
                "ai_marketing_graphql",
                "insertUpdateContactProfile",
                "Mutation",
                mutation_variables,
            )
            contact_profile = humps.decamelize(result["contactProfile"])

            return contact_profile
        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise e

    # * MCP Function.
    def data_collect(self, **arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Collect data and create/update contact profile."""
        try:
            self.logger.info(f"Arguments: {arguments}")
            data_collect_dataset = {
                k: (", ".join(v) if isinstance(v, list) else v)
                for k, v in Serializer.json_loads(
                    arguments["data_collect_dataset"]
                ).items()
                if v is not None
            }
            place_uuid = data_collect_dataset.pop("place_uuid", None)
            email = data_collect_dataset.pop("email", None)
            first_name = data_collect_dataset.pop("first_name", None)
            last_name = data_collect_dataset.pop("last_name", None)
            data_collect_dataset.update(
                {
                    "sales_rep": self.setting.get("sales_rep", "Marketing Team"),
                }
            )
            variables = {
                "email": email,
            }

            result = self._execute_graphql_query(
                "ai_marketing_graphql",
                "contactProfileList",
                "Query",
                {
                    "email": email,
                },
            )
            contact_uuid = None

            if result["total"] > 0:
                contact_profile = humps.decamelize(result["contactProfileList"][0])
                contact_uuid = contact_profile["contact_uuid"]

            variables = {
                "placeUuid": place_uuid,
                "email": email,
                "firstName": first_name,
                "lastName": last_name,
                "data": data_collect_dataset,
                "updatedBy": "Admin",
            }
            if contact_uuid:
                variables.update({"contactUuid": contact_uuid})

            result = self._execute_graphql_query(
                "ai_marketing_graphql",
                "insertUpdateContactProfile",
                "Mutation",
                variables,
            )
            contact_profile = humps.decamelize(result["contactProfile"])
            self.logger.info(f"Contact Profile: {contact_profile}")

            return {
                "contact_uuid": contact_profile["contact_uuid"],
                "sales_rep": self.setting.get("sales_rep", "Marketing Team"),
                "sales_rep_email": self.setting.get(
                    "sales_rep_email", "marketing@company.com"
                ),
            }
        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise e

    # * MCP Function.
    def submit_request(self, **arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Submit a contact request."""
        try:
            self.logger.info(f"Arguments: {arguments}")

            variables = {
                "placeUuid": arguments["place_uuid"],
                "contactUuid": arguments["contact_uuid"],
                "requestTitle": arguments["request_title"],
                "requestDetail": arguments["request_detail"],
                "updatedBy": "Admin",
            }
            result = self._execute_graphql_query(
                "ai_marketing_graphql",
                "insertUpdateContactRequest",
                "Mutation",
                variables,
            )
            contact_request = humps.decamelize(result["contactRequest"])

            return {"request_uuid": contact_request["request_uuid"]}
        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise e
