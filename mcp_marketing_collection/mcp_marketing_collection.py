#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

__author__ = "bibow"

import logging
import traceback
from typing import Any, Dict, List, Optional

import boto3
import humps
from botocore.exceptions import BotoCoreError, NoCredentialsError

from shopify_connector import ShopifyConnector
from silvaengine_utility import Utility

# Global variable to store schemas
_SCHEMAS = {}


def initialize_aws_lambda_client(setting: Dict[str, Any]) -> Any:
    """
    Initialize the AWS Lambda client using the provided credentials or default configuration.
    """
    region_name = setting.get("region_name")
    aws_access_key_id = setting.get("aws_access_key_id")
    aws_secret_access_key = setting.get("aws_secret_access_key")

    if region_name and aws_access_key_id and aws_secret_access_key:
        aws_lambda = boto3.client(
            "lambda",
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
    else:
        aws_lambda = boto3.client("lambda")
    return aws_lambda


def fetch_graphql_schema(
    logger: Any,
    endpoint_id: str,
    function_name: str,
    setting: Dict[str, Any],
) -> Dict[str, Any]:
    global _SCHEMAS
    if _SCHEMAS.get(function_name) is None:
        _SCHEMAS[function_name] = Utility.fetch_graphql_schema(
            logger,
            endpoint_id,
            function_name,
            setting=setting,
            test_mode=setting.get("test_mode"),
            aws_lambda=initialize_aws_lambda_client(setting),
        )
    return _SCHEMAS[function_name]


def execute_graphql_query(
    logger: Any,
    endpoint_id: str,
    function_name: str,
    operation_name: str,
    operation_type: str,
    variables: Dict[str, Any],
    setting: Dict[str, Any],
) -> Dict[str, Any]:
    schema = fetch_graphql_schema(logger, endpoint_id, function_name, setting)
    return Utility.execute_graphql_query(
        logger,
        endpoint_id,
        function_name,
        Utility.generate_graphql_operation(operation_name, operation_type, schema),
        variables,
        setting=setting,
        test_mode=setting.get("test_mode"),
        aws_lambda=initialize_aws_lambda_client(setting),
    )


def get_google_place_setting(
    logger: logging.Logger, setting: Dict[str, Any], **arguments: Dict[str, Any]
) -> Dict[str, Any]:
    """Get Google Place API settings for marketing collection."""
    try:
        logger.info(f"Arguments: {arguments}")
        logger.info(f"Setting: {setting}")
        return {
            "keyword": setting["keyword"],
            "google_api_key": setting["google_api_key"],
        }
    except Exception as e:
        log = traceback.format_exc()
        logger.error(log)
        raise e


def get_question_group(
    logger: logging.Logger, setting: Dict[str, Any], **arguments: Dict[str, Any]
) -> Dict[str, Any]:
    """Get question group for a place."""
    try:
        logger.info(f"Arguments: {arguments}")
        endpoint_id = arguments["endpoint_id"]
        place_uuid = arguments["place_uuid"]
        result = execute_graphql_query(
            logger,
            endpoint_id,
            "ai_marketing_graphql",
            "questionGroupList",
            "Query",
            {"placeUuid": place_uuid},
            setting,
        )
        if result["questionGroupList"]["total"] == 0:
            result = execute_graphql_query(
                logger,
                endpoint_id,
                "ai_marketing_graphql",
                "questionGroupList",
                "Query",
                {"region": "*"},
                setting,
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
        logger.error(log)
        raise e


def get_contact_profile(
    logger: logging.Logger, setting: Dict[str, Any], **arguments: Dict[str, Any]
) -> Dict[str, Any]:
    """Get or create contact profile."""
    try:
        logger.info(f"Arguments: {arguments}")
        endpoint_id = arguments["endpoint_id"]
        contact = arguments["contact"]
        place = arguments["place"]
        variables = {
            "email": contact["email"],
            "placeUuid": place["place_uuid"],
        }
        result = execute_graphql_query(
            logger,
            endpoint_id,
            "ai_marketing_graphql",
            "contactProfileList",
            "Query",
            variables,
            setting,
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
                    "sales_rep": setting["sales_rep"],
                },
                "updatedBy": "Admin",
            }
        )
        result = execute_graphql_query(
            logger,
            endpoint_id,
            "ai_marketing_graphql",
            "insertUpdateContactProfile",
            "Mutation",
            variables,
            setting,
        )
        contact_profile = humps.decamelize(
            result["insertUpdateContactProfile"]["contactProfile"]
        )

        return contact_profile
    except Exception as e:
        log = traceback.format_exc()
        logger.error(log)
        raise e


def data_collect(
    logger: logging.Logger, setting: Dict[str, Any], **arguments: Dict[str, Any]
) -> Dict[str, Any]:
    """Collect data and create/update contact profile."""
    try:
        logger.info(f"Arguments: {arguments}")
        endpoint_id = arguments["endpoint_id"]
        place_uuid = arguments["place_uuid"]
        data_collect_dataset = {
            k: (", ".join(v) if isinstance(v, list) else v)
            for k, v in Utility.json_loads(arguments["data_collect_dataset"]).items()
            if v is not None
        }
        email = data_collect_dataset.pop("email", None)
        first_name = data_collect_dataset.pop("first_name", None)
        last_name = data_collect_dataset.pop("last_name", None)
        data_collect_dataset.update({"sales_rep": setting["sales_rep"]})

        result = execute_graphql_query(
            logger,
            endpoint_id,
            "ai_marketing_graphql",
            "contactProfileList",
            "Query",
            {
                "placeUuid": place_uuid,
                "email": email,
            },
            setting,
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

        result = execute_graphql_query(
            logger,
            endpoint_id,
            "ai_marketing_graphql",
            "insertUpdateContactProfile",
            "Mutation",
            variables,
            setting,
        )
        contact_profile = humps.decamelize(
            result["insertUpdateContactProfile"]["contactProfile"]
        )
        logger.info(f"Contact Profile: {contact_profile}")

        return {
            "contact_uuid": contact_profile["contact_uuid"],
            "sales_rep": setting["sales_rep"],
        }
    except Exception as e:
        log = traceback.format_exc()
        logger.error(log)
        raise e


def submit_request(
    logger: logging.Logger, setting: Dict[str, Any], **arguments: Dict[str, Any]
) -> Dict[str, Any]:
    """Submit a contact request."""
    try:
        logger.info(f"Arguments: {arguments}")
        endpoint_id = arguments["endpoint_id"]

        variables = {
            "placeUuid": arguments["place_uuid"],
            "contactUuid": arguments["contact_uuid"],
            "requestTitle": arguments["request_title"],
            "requestDetail": arguments["request_detail"],
            "updatedBy": "Admin",
        }
        result = execute_graphql_query(
            logger,
            endpoint_id,
            "ai_marketing_graphql",
            "insertUpdateContactRequest",
            "Mutation",
            variables,
            setting,
        )
        contact_request = humps.decamelize(
            result["insertUpdateContactRequest"]["contactRequest"]
        )

        return {"request_uuid": contact_request["request_uuid"]}
    except Exception as e:
        log = traceback.format_exc()
        logger.error(log)
        raise e


def get_shopify_product_data(
    logger: logging.Logger, setting: Dict[str, Any], **arguments: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Get Shopify product data for promotion."""
    promotion_products = setting.get("promotion_products", [])
    if len(promotion_products) == 0:
        return []

    try:
        shopify_connector = ShopifyConnector(logger, **setting)
        product_handles = {
            product_info["handle"]: product_info
            for product_info in promotion_products
            if product_info.get("handle") is not None
        }
        products = shopify_connector.find_products_by_attributes(
            {"handle": ",".join(list(set(product_handles.keys())))}
        )
        if products is not None:
            products_data = []
            for product in products:
                if product.handle in product_handles:
                    default_variant = None
                    selected_variant = None
                    for variant in product.variants:
                        default_variant = variant
                        if variant.id == product_handles[product.handle]["variant_id"]:
                            selected_variant = variant
                            break
                    if selected_variant is None:
                        selected_variant = default_variant
                    if len(product.variants) == 1:
                        title = "{title}".format(title=product.title)
                    else:
                        title = "{title} - {variant_title}".format(
                            title=product.title,
                            variant_title=selected_variant.title,
                        )
                    price = selected_variant.price
                    products_data.append(
                        {
                            "title": title,
                            "handle": product.handle,
                            "price": price,
                            "body_html": product.body_html,
                        }
                    )

            return products_data
    except Exception as e:
        log = traceback.format_exc()
        logger.error(log)
        raise e
    return []


def place_shopify_draft_order(
    logger: logging.Logger, setting: Dict[str, Any], **arguments: Dict[str, Any]
) -> str:
    """Place a Shopify draft order."""
    promotion_products = setting.get("promotion_products", [])
    if len(promotion_products) == 0:
        raise Exception("No promotion products found")
    try:
        shopify_connector = ShopifyConnector(logger, **setting)
        contact = arguments["contact"]
        email = contact["email"]
        shipping_address = arguments.get("shipping_address")
        billing_address = arguments.get("billing_address")

        items = setting["promotion_products"]
        line_items = []
        for item in items:
            if item.get("variant_id") is None:
                continue
            line_items.append(
                {
                    "variant_id": item.get("variant_id"),
                    "quantity": item.get("quantity", 1),
                }
            )
        return shopify_connector.create_draft_order(
            email, line_items, shipping_address, billing_address
        )
    except Exception as e:
        log = traceback.format_exc()
        logger.error(log)
        raise e
