from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue

from target_hyphen.sinks import (
    PaymentsSink,
)


class TargetHyphen(TargetHotglue):
    name = "target-hyphen"
    SINK_TYPES = [PaymentsSink]
    MAX_PARALLELISM = 1

    config_jsonschema = th.PropertiesList(
        th.Property("access_key", th.StringType, required=True),
        th.Property("secret_key", th.StringType, required=True),
        th.Property("url", th.StringType, required=False),
        th.Property("party_id", th.StringType, required=True),
        th.Property("app_id", th.StringType, required=True),
    ).to_dict()


if __name__ == "__main__":
    TargetHyphen.cli()
