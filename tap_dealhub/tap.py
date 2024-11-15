from singer_sdk import Tap

from singer_sdk.typing import (
    IntegerType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_dealhub.streams import (
    Quotes,
)

class TapDealHub(Tap):
    name = 'tap-dealhub'

    config_jsonschema = PropertiesList(
        Property(
            'url_base',
            StringType,
            default='https://service-us1.dealhub.io',
            description='The base url for the DealHub API',
        ),
        Property(
            'bearer_token',
            StringType,
            secret=True,
            description='Bearer token used for manual authentication',
        ),
        Property(
            'page_size',
            IntegerType,
            default=25,
            description='The number of results to request per page. Must be in the range 1-50.',
        ),
        Property(
            'stream_type_conformance',
            StringType,
            default='none',
            description='The level of type conformance to apply to streams '
            '(see: https://sdk.meltano.com/en/latest/classes/singer_sdk.Stream.html#singer_sdk.Stream.TYPE_CONFORMANCE_LEVEL). '
            'Defaults to none. Must be one of: none, root_only, recursive',
            allowed_values=['none', 'root_only', 'recursive'],
        ),
        Property(
            'stream_maps',
            ObjectType(),
            description='Inline stream maps (see: https://sdk.meltano.com/en/latest/stream_maps.html)',
        ),
        Property(
            'stream_maps_config',
            ObjectType(),
            description='Inline stream maps config (see: https://sdk.meltano.com/en/latest/stream_maps.html)',
        ),
    ).to_dict()

    def discover_streams(self):
        return [
            Quotes(self),
        ]

if __name__ == '__main__':
    TapDealHub.cli()
