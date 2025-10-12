#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

__author__ = "bibow"

import logging
import re
import traceback
from typing import Any, Dict, List

import boto3
import humps
import pendulum

from silvaengine_utility import Utility

MCP_CONFIGURATION = {
    "tools": [
        {
            "name": "get_google_place_setting",
            "description": "Get Google Place API settings for marketing collection",
            "inputSchema": {"type": "object", "properties": {}, "required": []},
            "annotations": None,
        },
        {
            "name": "get_place",
            "description": "Get or create a place by location and business details",
            "inputSchema": {
                "type": "object",
                "properties": {
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
                "required": ["region", "latitude", "longitude", "address"],
            },
            "annotations": None,
        },
        {
            "name": "get_question_group",
            "description": "Get question group for a place",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "place_uuid": {"type": "string", "description": "UUID of the place"}
                },
                "required": ["place_uuid"],
            },
            "annotations": None,
        },
        {
            "name": "get_contact_profile",
            "description": "Get or create contact profile",
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
            "description": "Collect data and create/update contact profile",
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
            "description": "Submit a contact request",
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
        {
            "name": "get_shopify_product_data",
            "description": "Get Shopify product data for promotion",
            "inputSchema": {"type": "object", "properties": {}, "required": []},
            "annotations": None,
        },
        {
            "name": "place_shopify_draft_order",
            "description": "Place a Shopify draft order",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "contact": {
                        "type": "object",
                        "description": "Contact information",
                        "properties": {"email": {"type": "string"}},
                        "required": ["email"],
                    },
                    "shipping_address": {
                        "type": "object",
                        "description": "Shipping address information",
                        "properties": {
                            "address1": {"type": "string"},
                            "address2": {"type": "string"},
                            "city": {"type": "string"},
                            "province_code": {"type": "string"},
                            "province": {"type": "string"},
                            "zip": {"type": "string"},
                            "country": {"type": "string"},
                            "country_code": {"type": "string"},
                            "company": {"type": "string"},
                            "first_name": {"type": "string"},
                            "last_name": {"type": "string"},
                            "phone": {"type": "string"},
                        },
                    },
                    "billing_address": {
                        "type": "object",
                        "description": "Billing address information",
                        "properties": {
                            "address1": {"type": "string"},
                            "address2": {"type": "string"},
                            "city": {"type": "string"},
                            "province_code": {"type": "string"},
                            "province": {"type": "string"},
                            "zip": {"type": "string"},
                            "country": {"type": "string"},
                            "country_code": {"type": "string"},
                            "company": {"type": "string"},
                            "first_name": {"type": "string"},
                            "last_name": {"type": "string"},
                            "phone": {"type": "string"},
                        },
                    },
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "variant_id": {"type": "string"},
                                "quantity": {"type": "integer"},
                            },
                        },
                        "description": "Array of line items for the draft order",
                    },
                },
                "required": ["contact"],
            },
            "annotations": None,
        },
    ],
    "resources": [],
    "prompts": [],
    "module_links": [
        {
            "type": "tool",
            "name": "get_google_place_setting",
            "module_name": "mcp_marketing_collection",
            "class_name": "MCPMarketingCollection",
            "function_name": "get_google_place_setting",
            "return_type": "text",
        },
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
            "name": "get_question_group",
            "module_name": "mcp_marketing_collection",
            "class_name": "MCPMarketingCollection",
            "function_name": "get_question_group",
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
        {
            "type": "tool",
            "name": "get_shopify_product_data",
            "module_name": "mcp_marketing_collection",
            "class_name": "MCPMarketingCollection",
            "function_name": "get_shopify_product_data",
            "return_type": "text",
        },
        {
            "type": "tool",
            "name": "place_shopify_draft_order",
            "module_name": "mcp_marketing_collection",
            "class_name": "MCPMarketingCollection",
            "function_name": "place_shopify_draft_order",
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
        self._schemas = {}
        self._aws_lambda = self._initialize_aws_lambda_client(**setting)

    @property
    def endpoint_id(self) -> str:
        return self._endpoint_id

    @endpoint_id.setter
    def endpoint_id(self, value: str):
        self._endpoint_id = value

    def _initialize_aws_lambda_client(self, **setting: Dict[str, Any]) -> boto3.client:
        region_name = setting.get("region_name")
        aws_access_key_id = setting.get("aws_access_key_id")
        aws_secret_access_key = setting.get("aws_secret_access_key")
        if region_name and aws_access_key_id and aws_secret_access_key:
            return boto3.client(
                "lambda",
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
        else:
            return boto3.client("lambda")

    def _fetch_graphql_schema(
        self,
        function_name: str,
    ) -> Dict[str, Any]:
        try:
            if self._schemas.get(function_name) is None:
                self._schemas[function_name] = Utility.fetch_graphql_schema(
                    self.logger,
                    self.endpoint_id,
                    function_name,
                    setting=self.setting,
                    execute_mode=self.setting.get("execute_mode"),
                    aws_lambda=self._aws_lambda,
                )
            return self._schemas[function_name]
        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise Exception(
                f"Failed to fetch GraphQL schema: {function_name}/{self.endpoint_id}. Please check the configuration and ensure all required settings are properly. Error: {e}"
            )

    def _execute_graphql_query(
        self,
        function_name: str,
        operation_name: str,
        operation_type: str,
        variables: Dict[str, Any],
    ) -> Dict[str, Any]:
        try:
            schema = self._fetch_graphql_schema(function_name)
            query = Utility.generate_graphql_operation(
                operation_name, operation_type, schema
            )
            self.logger.info(f"Query: {query}/{function_name}")
            return Utility.execute_graphql_query(
                self.logger,
                self.endpoint_id,
                function_name,
                query,
                variables,
                setting=self.setting,
                execute_mode=self.setting.get("execute_mode"),
                aws_lambda=self._aws_lambda,
            )
        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise Exception(
                f"Failed to execute GraphQL query ({function_name}/{self.endpoint_id}). Error: {e}"
            )

    # * MCP Function.
    def get_google_place_setting(self, **arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Get Google Place API settings for marketing collection."""
        try:
            self.logger.info(f"Arguments: {arguments}")
            self.logger.info(f"Setting: {self.setting}")
            return {
                "keyword": self.setting["keyword"],
                "google_api_key": self.setting["google_api_key"],
            }
        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise e

    # * MCP Function.
    def get_place(self, **arguments: Dict[str, any]) -> Dict[str, Any]:
        """ """
        try:
            self.logger.info(f"Arguments: {arguments}")
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
            if result["placeList"]["total"] > 0:
                place = humps.decamelize(result["placeList"]["placeList"][0])
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
            place = humps.decamelize(result["insertUpdatePlace"]["place"])

            return place

        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise e

    # * MCP Function.
    def get_question_group(self, **arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Get question group for a place."""
        try:
            self.logger.info(f"Arguments: {arguments}")
            place_uuid = arguments["place_uuid"]
            result = self._execute_graphql_query(
                "ai_marketing_graphql",
                "questionGroupList",
                "Query",
                {"placeUuid": place_uuid},
            )
            if result["questionGroupList"]["total"] == 0:
                result = self._execute_graphql_query(
                    "ai_marketing_graphql",
                    "questionGroupList",
                    "Query",
                    {"region": "*"},
                )

            question_groups = result["questionGroupList"]["questionGroupList"]
            question_groups = [
                humps.decamelize(
                    {
                        k: v
                        for k, v in question_group.items()
                        if v is not None
                        and k
                        not in [
                            "endpointId",
                            "questionCriteria",
                            "region",
                            "updatedAt",
                            "updatedBy",
                            "createdAt",
                        ]
                    }
                )
                for question_group in question_groups
            ]

            return sorted(question_groups, key=lambda x: x["weight"])[0]
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
            if result["contactProfileList"]["total"] > 0:
                contact_profile = humps.decamelize(
                    result["contactProfileList"]["contactProfileList"][0]
                )
                variables.update({"contactUuid": contact_profile["contact_uuid"]})

                if all(
                    [
                        contact_profile.get("first_name") == contact["first_name"],
                        contact_profile.get("last_name") == contact["last_name"],
                        contact_profile["place"].get("place_uuid")
                        == place.get("place_uuid"),
                    ]
                ):
                    return contact_profile

            variables.update(
                {
                    "firstName": contact["first_name"],
                    "lastName": contact["last_name"],
                    "placeUuid": place.get("place_uuid"),
                    "updatedBy": "Admin",
                }
            )
            result = self._execute_graphql_query(
                "ai_marketing_graphql",
                "insertUpdateContactProfile",
                "Mutation",
                variables,
            )
            contact_profile = humps.decamelize(
                result["insertUpdateContactProfile"]["contactProfile"]
            )

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
                for k, v in Utility.json_loads(
                    arguments["data_collect_dataset"]
                ).items()
                if v is not None
            }
            place_uuid = data_collect_dataset.pop("place_uuid", None)
            email = data_collect_dataset.pop("email", None)
            first_name = data_collect_dataset.pop("first_name", None)
            last_name = data_collect_dataset.pop("last_name", None)
            data_collect_dataset.update({"sales_rep": self.setting["sales_rep"]})

            result = self._execute_graphql_query(
                "ai_marketing_graphql",
                "contactProfileList",
                "Query",
                {
                    "email": email,
                },
            )

            contact_uuid = None
            if result["contactProfileList"]["total"] > 0:
                contact_profile = humps.decamelize(
                    result["contactProfileList"]["contactProfileList"][0]
                )
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
            contact_profile = humps.decamelize(
                result["insertUpdateContactProfile"]["contactProfile"]
            )
            self.logger.info(f"Contact Profile: {contact_profile}")

            return {
                "contact_uuid": contact_profile["contact_uuid"],
                "sales_rep": self.setting["sales_rep"],
                "sales_rep_email": self.setting["sales_rep_email"],
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
            contact_request = humps.decamelize(
                result["insertUpdateContactRequest"]["contactRequest"]
            )

            return {"request_uuid": contact_request["request_uuid"]}
        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise e

    # * MCP Function.
    def place_shopify_draft_order(self, **arguments: Dict[str, Any]) -> str:
        """Place a Shopify draft order."""
        try:
            contact = arguments["contact"]
            email = contact["email"]
            shipping_address = arguments.get("shipping_address")
            billing_address = arguments.get("billing_address")

            items = arguments.get("items", [])
            line_items = []
            for item in items:
                if item.get("variant_id") is None:
                    continue
                variant_id = None
                match = re.search(r"\d+", item.get("variant_id"))
                if match:
                    variant_id = match.group()
                if variant_id is None:
                    continue
                line_items.append(
                    {
                        "variant_id": variant_id,
                        "quantity": item.get("quantity", 1),
                    }
                )
            variables = {
                "shop": self.endpoint_id,
                "email": email,
                "lineItems": line_items,
                "shippingAddress": shipping_address,
                "billingAddress": billing_address,
            }
            result = self._execute_graphql_query(
                "shopify_app_engine_graphql",
                "createDraftOrder",
                "Mutation",
                variables,
            )
            if result.get("createDraftOrder", {}).get("draftOrder"):
                return result.get("createDraftOrder", {}).get("draftOrder")
            return None
        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise e
