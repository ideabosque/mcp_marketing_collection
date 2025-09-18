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
                "required": ["contact", "place"],
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
        self._schemas = {}
        self._aws_lambda = self._initialize_aws_lambda_client(**setting)

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
        endpoint_id: str,
        function_name: str,
    ) -> Dict[str, Any]:
        if self._schemas.get(function_name) is None:
            self._schemas[function_name] = Utility.fetch_graphql_schema(
                self.logger,
                endpoint_id,
                function_name,
                setting=self.setting,
                test_mode=self.setting.get("test_mode"),
                aws_lambda=self._aws_lambda,
            )
        return self._schemas[function_name]

    def _execute_graphql_query(
        self,
        endpoint_id: str,
        function_name: str,
        operation_name: str,
        operation_type: str,
        variables: Dict[str, Any],
    ) -> Dict[str, Any]:
        schema = self._fetch_graphql_schema(endpoint_id, function_name)
        return Utility.execute_graphql_query(
            self.logger,
            endpoint_id,
            function_name,
            Utility.generate_graphql_operation(operation_name, operation_type, schema),
            variables,
            setting=self.setting,
            test_mode=self.setting.get("test_mode"),
            aws_lambda=self._aws_lambda,
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
    def get_question_group(self, **arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Get question group for a place."""
        try:
            self.logger.info(f"Arguments: {arguments}")
            endpoint_id = arguments["endpoint_id"]
            place_uuid = arguments["place_uuid"]
            result = self._execute_graphql_query(
                endpoint_id,
                "ai_marketing_graphql",
                "questionGroupList",
                "Query",
                {"placeUuid": place_uuid},
            )
            if result["questionGroupList"]["total"] == 0:
                result = self._execute_graphql_query(
                    endpoint_id,
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
            endpoint_id = arguments["endpoint_id"]
            contact = arguments["contact"]
            place = arguments["place"]
            variables = {
                "email": contact["email"],
                "placeUuid": place["place_uuid"],
            }
            result = self._execute_graphql_query(
                endpoint_id,
                "ai_marketing_graphql",
                "contactProfileList",
                "Query",
                variables,
            )
            if result["contactProfileList"]["total"] > 0:
                contact_profile = humps.decamelize(
                    result["contactProfileList"]["contactProfileList"][0]
                )
                return contact_profile

            variables.update(
                {
                    "firstName": contact["first_name"],
                    "lastName": contact["last_name"],
                    "data": {
                        "sales_rep": self.setting["sales_rep"],
                    },
                    "updatedBy": "Admin",
                }
            )
            result = self._execute_graphql_query(
                endpoint_id,
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
            endpoint_id = arguments["endpoint_id"]
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
                endpoint_id,
                "ai_marketing_graphql",
                "contactProfileList",
                "Query",
                {
                    "placeUuid": place_uuid,
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
                endpoint_id,
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
            endpoint_id = arguments["endpoint_id"]

            variables = {
                "placeUuid": arguments["place_uuid"],
                "contactUuid": arguments["contact_uuid"],
                "requestTitle": arguments["request_title"],
                "requestDetail": arguments["request_detail"],
                "updatedBy": "Admin",
            }
            result = self._execute_graphql_query(
                endpoint_id,
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
    def get_shopify_product_data(
        self, **arguments: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Get Shopify product data for promotion."""
        promotion_products = self.setting.get("promotion_products", [])
        if len(promotion_products) == 0:
            return []

        endpoint_id = arguments.get("endpoint_id", None)
        if endpoint_id is None:
            return []
        try:
            product_handles = {
                product_info["handle"]: product_info
                for product_info in promotion_products
                if product_info.get("handle") is not None
            }
            variables = {
                "shop": endpoint_id,
                "attributes": {"handle": ",".join(list(set(product_handles.keys())))},
            }
            result = self._execute_graphql_query(
                self.setting.get("shopify_endpoint_id", "openai"),
                "shopify_app_engine_graphql",
                "productList",
                "Query",
                variables,
            )

            products = result.get("productList", {}).get("productList", [])
            if len(products) > 0:
                products_data = []
                for product in products:
                    if product.get("handle") in product_handles:
                        default_variant = None
                        selected_variant = None
                        for variant in product.get("variants", []):
                            default_variant = variant
                            if (
                                variant.get("id")
                                == product_handles[product.get("handle")]["variant_id"]
                            ):
                                selected_variant = variant
                                break
                        if selected_variant is None:
                            selected_variant = default_variant
                        if len(product.get("variants", [])) == 1:
                            title = "{title}".format(title=product.get("title"))
                        else:
                            title = "{title} - {variant_title}".format(
                                title=product.get("title"),
                                variant_title=selected_variant.get("title"),
                            )
                        price = selected_variant.get("price")
                        products_data.append(
                            {
                                "title": title,
                                "handle": product.get("handle"),
                                "price": price,
                                "body_html": product.get("bodyHtml"),
                            }
                        )
                return products_data
        except Exception as e:
            log = traceback.format_exc()
            self.logger.error(log)
            raise e
        return []

    # * MCP Function.
    def place_shopify_draft_order(self, **arguments: Dict[str, Any]) -> str:
        """Place a Shopify draft order."""
        promotion_products = self.setting.get("promotion_products", [])
        if len(promotion_products) == 0:
            raise Exception("No promotion products found")

        endpoint_id = arguments.get("endpoint_id", None)
        if endpoint_id is None:
            raise Exception("No endpoint id provided")

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
                "shop": endpoint_id,
                "email": email,
                "lineItems": line_items,
                "shippingAddress": shipping_address,
                "billingAddress": billing_address,
            }
            result = self._execute_graphql_query(
                self.setting.get("shopify_endpoint_id", "openai"),
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
