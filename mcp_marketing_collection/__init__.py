#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = "bibow"

from .mcp_marketing_collection import (
    get_google_place_setting,
    get_question_group,
    get_contact_profile,
    data_collect,
    submit_request,
    get_shopify_product_data,
    place_shopify_draft_order,
)

__all__ = [
    "get_google_place_setting",
    "get_question_group", 
    "get_contact_profile",
    "data_collect",
    "submit_request",
    "get_shopify_product_data",
    "place_shopify_draft_order",
]