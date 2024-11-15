from functools import cached_property
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers._typing import TypeConformanceLevel
from singer_sdk.pagination import BaseOffsetPaginator
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

import requests
import logging

from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

class DealHubPaginator(BaseOffsetPaginator):
    def has_more(self, response):
        return response.json()['info']['more_results_matching_the_request']

class DealHubStream(RESTStream):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.TYPE_CONFORMANCE_LEVEL = {
            'none': TypeConformanceLevel.NONE,
            'root_only': TypeConformanceLevel.ROOT_ONLY,
            'recursive': TypeConformanceLevel.RECURSIVE,
        }.get(self.config['stream_type_conformance'])

    @property
    def url_base(self):
        return self.config['url_base']

    @cached_property
    def authenticator(self):
        return BearerTokenAuthenticator(self, self.config['bearer_token'])

    def get_new_paginator(self):
        return DealHubPaginator(start_value=0, page_size=self.config['page_size'])

    def get_url_params(self, context, next_page_token):
        return {
            'limit': self.config['page_size'],
            'offset': next_page_token,
        }

    # Quotes with deleted approvers will always error on request and stop the pipeline
    # As a temporary workaround, retry failed requests once without approvers otherwise skip entirely with dummy record
    def request_decorator(self, func):
        def request_handler(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (
                    ConnectionResetError,
                    FatalAPIError,
                    RetriableAPIError,
                    requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ContentDecodingError,
                ) as exception:
                logging.warning(f'Retrying downgraded request {args[0].url} on error: {exception}')
                args[0].url = args[0].url.replace('feature=all', 'feature=info&feature=summary&feature=deal_room_info&feature=line_items&feature=answers')
                try:
                    return func(*args, **kwargs)
                except (
                        ConnectionResetError,
                        FatalAPIError,
                        RetriableAPIError,
                        requests.exceptions.Timeout,
                        requests.exceptions.ConnectionError,
                        requests.exceptions.ChunkedEncodingError,
                        requests.exceptions.ContentDecodingError,
                    ) as exception:
                    logging.error(f'Skipping request {args[0].url} on error: {exception}')
                    unknown_response = requests.Response()
                    unknown_response.status_code = 200
                    unknown_response._content = b'{"info":{"more_results_matching_the_request":true},"quotes":[{"dealhub_quote_id":"unknown"}]}'
                    return unknown_response
        return request_handler

class Quotes(DealHubStream):
    name = 'quotes'
    path = '/api/v2/quotes'
    records_jsonpath = '$.quotes[*]'
    primary_keys = ['dealhub_quote_id']
    schema = PropertiesList(
        Property('dealhub_quote_id', StringType, description='DealHub unique quote ID', required=True),
        Property('status', StringType, description='Status of the quote.'),
        Property('quote_upgrade_required', BooleanType, description='Indicates if the quote needs to be “upgraded”. For the cases when this flag is ‘true’, the user should access this quote and evaluate its details in context of the new active Version and save the changes. Flag could be ‘true’ in case of DRAFT quote only'),
        Property('info', ObjectType(
            Property('external_opportunity_id', StringType, description='External opportunity ID (CRM opportunity ID as provided during create quote call)'),
            Property('external_quote_id', StringType, description='External quote ID (as provided during create quote call) '),
            Property('external_customer_id', StringType, description='External customer ID (CRM customer account ID)'),
            Property('customer_id', StringType, description='CPQ customer account ID'),
            Property('account_name', StringType, description='Account name (as provided during quote creation)'),
            Property('contact_name', StringType, description='Primary customer contact full name'),
            Property('contact_email', StringType, description='Primary customer contact email'),
            Property('dealhub_opportunity_id', StringType, description='DealHub opportunity ID'),
            Property('dealhub_quote_name', StringType, description='DealHub quote name'),
            Property('dealhub_quote_number', StringType, description='Quote ID that is unique per account and presented to user in CPQ UI'),
            Property('primary_quote', BooleanType, description='Primary quote flag'),
            Property('created_by', StringType, description='Login of the user who created the quote'),
            Property('creation_date', DateTimeType, description='GMT date&time of quote creation'),
            Property('submitted_by', StringType, description='Login of the user who submitted the quote (if specific quote is submitted)'),
            Property('submission_date', DateTimeType, description='GMT date&time of quote submission (if specific quote is submitted)'),
            Property('expiration_date', DateTimeType, description='Quote expiration date'),
            Property('won_date', DateTimeType, description='The date the quote was signed (all signers signed and quote status became ‘won’)'),
            Property('geo_code', StringType, description='Geographic code'),
            Property('currency', StringType, description='Quote currency [Currency format ISO-4217]'),
            Property('playbook', StringType, description='Playbook name'),
            Property('version_id', StringType, description='ID of the version in which the quote is managed [If the account is using the continuous-version mode, the version in which the quote is signed might differ from the version in which the quote was created.]'),
            additional_properties=StringType
        ), description='Quote general information'),
        Property('summary', ObjectType(
            Property('currency', StringType, description='Quote currency [Currency format ISO-4217]'),
            Property('total_list_price', NumberType, description='Total list price of quote'),
            Property('total_net_price', NumberType, description='Total net price of quote'),
            Property('total_discount', NumberType, description='Total discount of quote'),
            Property('sales_discount', NumberType, description='Total discount provided by seller'),
            additional_properties=StringType
        ), description='Quote summary'),
        Property('deal_room_info', ObjectType(
            Property('url', StringType, description='DealRoom URL'),
            additional_properties=StringType
        ), description='DealRoom information. Relevant for quotes in ‘published’ status. DealRoom information can be also retrieved for ‘won’ quotes if their signing process was managed via the DealRoom. In other scenarios the response for ‘deal_room_info’ will be empty'),
        Property('line_items', ArrayType(ObjectType(
            Property('id', StringType, description='Unique identifier of the line item instance in this quote'),
            Property('sku', StringType, description='Product catalog unique identifier'),
            Property('bundle_ref', StringType, description='Reference of the line item to its bundle (Bundle SKU). This field will have value if the line item is part of the bundle. For the bundle itself this field will be empty.'),
            Property('name', StringType, description='Product catalog name'),
            Property('primary_tag', StringType, description='Product primary tag'),
            Property('system_name', StringType, description='System name. Relevant in case of multi-system (aka multi-offer concept)'),
            Property('system_id', StringType, description='System ID. Relevant in case of multi-system (aka multi-offer concept)'),
            Property('group_name', StringType, description='Name of the group'),
            Property('msrp', NumberType, description='Line item MSRP'),
            Property('msrp_discount', NumberType, description='MSRP discount'),
            Property('list_price', NumberType, description='Line item list price'),
            Property('user_price', NumberType, description='End user price. Relevant for indirect sale (partners).'),
            Property('user_discount', NumberType, description='End user discount. Relevant for indirect sale (partners).'),
            Property('net_per_unit', NumberType, description='Unit net price'),
            Property('net_price', NumberType, description='Total net price of the line item (net per unit * product factors)'),
            Property('partner_discounts', ArrayType(ObjectType(
                Property('name', StringType, description='Partner program name'),
                Property('discount', NumberType, description='Discount percentage'),
                Property('price', NumberType, description='Price after partner discount'),
                additional_properties=StringType
            )), description='List of partner level discounts (indirect sale).'),
            Property('sales_discount', NumberType, description='Sales discount'),
            Property('total_discount', NumberType, description='Total discount'),
            Property('product_factors', ObjectType(additional_properties=StringType), description='List of all products factors (e.g. quantity, duration, etc) relevant for specific line item'),
            Property('attributes', ObjectType(additional_properties=StringType), description='List of line item attributes (aka proposal attributes), which differ per each SKU.'),
            additional_properties=StringType
        )), description='List of the quote line items.'),
        Property('answers', ArrayType(ObjectType(
            Property('system_name', StringType, description='Name of the system. Relevant in case of multi-system (aka multi-offer concept)'),
            Property('system_id', StringType, description='System ID. Relevant in case of multi-system (aka multi-offer concept)'),
            Property('group_id', StringType, description='Group ID'),
            Property('group_name', StringType, description='Group name'),
            Property('question_id', StringType, description='Question ID'),
            Property('question', StringType, description='Question text'),
            Property('answer', StringType, description='The answer [The response to a Date-type question will be provided in a format of yyyy-mm-dd hh:mm:ss]'),
            additional_properties=StringType
        )), description='List of playbook answers in context of specific quote. In case answer is a textList with multi-selection, multiple values selected that were selected by user will be separated by semicolon \';\''),
        Property('approvals', ArrayType(ObjectType(
            Property('reviewer', StringType, description='Login of the reviewer'),
            Property('step', IntegerType, description='Approval workflow step'),
            Property('reason', ArrayType(StringType), description='List of approval reasons'),
            Property('status', StringType, description='Status of the specific step within approval flow [“waiting”, “rejected”, “approved”]'),
            Property('impersonated_by', StringType, description='Login of the person who approved/rejected quote on behalf of approver'),
            Property('review_date', DateTimeType, description='Date of approval/rejection. Empty if status == “waiting”'),
            Property('reviewer_comment', StringType, description='Note added by reviewer'),
            additional_properties=StringType
        )), description='List of quote approval flows. If explicitly requested for the quote that didn’t require any approval workflow - system will return empty list.'),
        additional_properties=StringType
    ).to_dict()

    def get_url_params(self, context, next_page_token):
        return super().get_url_params(context, next_page_token) | { 'feature': 'all' }
