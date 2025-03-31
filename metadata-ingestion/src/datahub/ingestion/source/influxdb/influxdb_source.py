import base64
from typing import Iterable, List, Optional

import requests
from pydantic import Field, SecretStr

import datahub.emitter.mce_builder as builder
from datahub.configuration.source_common import PlatformInstanceConfigMixin
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemalessClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
)


class InfluxDBSourceConfig(StatefulIngestionConfigBase, PlatformInstanceConfigMixin):
    host: str = Field(
        description="InfluxDB URL (without trailing slash, can contain port with semicolon)",
    )
    token: Optional[SecretStr] = Field(description="InfluxDB authentication token")
    org: Optional[str] = Field(
        default="my-org", description="InfluxDB organization name"
    )
    username: Optional[SecretStr] = Field(
        description="InfluxDB authentication username"
    )
    password: Optional[SecretStr] = Field(
        description="InfluxDB authentication password"
    )


class InfluxDBReport(StaleEntityRemovalSourceReport):
    pass


@platform_name("InfluxDB")
@config_class(InfluxDBSourceConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class InfluxDBSource(StatefulIngestionSourceBase):
    """
    Experimental InfluxDB source for DataHub.
    Ingests InfluxDB buckets as datasets and their measurements as schema.
    """

    def __init__(self, config: InfluxDBSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.report = InfluxDBReport()
        self.platform = "influxdb"

        if self.source_config.token:
            self.headers = {
                "Authorization": f"Token {self.source_config.token.get_secret_value()}",
                "Content-Type": "application/json",
            }

        elif self.source_config.username and self.source_config.password:
            # Encode the credentials
            credentials = f"{self.source_config.username.get_secret_value()}:{self.source_config.password.get_secret_value()}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()

            self.headers = {
                "Authorization": f"Basic {encoded_credentials}",
                "Content-Type": "application/json",
            }
        else:
            self.headers = {}
            self.report.report_failure(
                "No valid authentication credentials provided. Should be token or username/password"
            )

    @classmethod
    def create(cls, config_dict, ctx):
        config = InfluxDBSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def get_report(self) -> StatefulIngestionReport:
        return self.report

    def get_measurements(self, bucket: str):
        query = "SHOW MEASUREMENTS"

        payload = {"query": query, "org": self.source_config.org, "bucket": bucket}

        try:
            response = requests.post(
                f"{self.source_config.host}/api/v2/query",
                headers=self.headers,
                json=payload,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.report.warning(
                f"Failed to fetch measurements for bucket {bucket}: {str(e)}"
            )
            return []

        return response.json().get("results", [])

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # Initialize data_platform_instance with a default value
        data_platform_instance = None
        if self.source_config.platform_instance:
            data_platform_instance = DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=make_dataplatform_instance_urn(
                    self.platform, self.source_config.platform_instance
                ),
            )

        try:
            response = requests.get(
                f"{self.source_config.host}/query?q=SHOW DATABASES",
                headers=self.headers,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.report.report_failure(f"Failed to fetch databases: {str(e)}")
            return

        # Extract the list of databases
        databases = [
            db[0]  # Each database name is the first element in the inner lists
            for result in response.json().get("results", [])
            for series in result.get("series", [])
            if series.get("name")
            == "databases"  # Ensure we are extracting the correct series
            for db in series.get("values", [])
        ]

        for db in databases:
            self.report.info(f"Processing database {db}")

            try:
                response = requests.get(
                    f"{self.source_config.host}/query?q=SHOW MEASUREMENTS&db={db}",
                    headers=self.headers,
                )
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                self.report.report_failure(f"Failed to fetch databases: {str(e)}")
                return

            # Extract the list of databases
            measurements = [
                measurement[
                    0
                ]  # Each database name is the first element in the inner lists
                for result in response.json().get("results", [])
                for series in result.get("series", [])
                if series.get("name")
                == "measurements"  # Ensure we are extracting the correct series
                for measurement in series.get("values", [])
            ]

            for measurement in measurements:
                self.report.info(f"Processing measurement {measurement}")

                urn = builder.make_dataset_urn(
                    platform=self.platform,
                    name=f"{db}.{measurement}",  # Include both database and measurement
                    env="PROD",  # Change to "DEV" if needed
                )

                field_response = requests.get(
                    f"{self.source_config.host}/query?q=SHOW FIELD KEYS FROM {measurement}&db={db}",
                    headers=self.headers,
                )
                tag_response = requests.get(
                    f"{self.source_config.host}/query?q=SHOW TAG KEYS FROM {measurement}&db={db}",
                    headers=self.headers,
                )

                schema_fields: List[SchemaFieldClass] = []

                # Process field keys
                if field_response.status_code == 200:
                    field_data = field_response.json()
                    for result in field_data.get("results", []):
                        for series in result.get("series", []):
                            if series.get("name") == measurement:
                                for row in series.get("values", []):
                                    schema_fields.append(
                                        SchemaFieldClass(
                                            fieldPath=row[0],  # Field name
                                            type=SchemaFieldDataTypeClass(
                                                type=NumberTypeClass()
                                            ),
                                            nativeDataType=row[1],  # Type from InfluxDB
                                            description=f"Field from measurement {measurement}",
                                        )
                                    )

                # Process tag keys
                if tag_response.status_code == 200:
                    tag_data = tag_response.json()
                    for result in tag_data.get("results", []):
                        for series in result.get("series", []):
                            if series.get("name") == measurement:
                                for row in series.get("values", []):
                                    schema_fields.append(
                                        SchemaFieldClass(
                                            fieldPath=row[0],  # Tag name
                                            type=SchemaFieldDataTypeClass(
                                                type=StringTypeClass()
                                            ),
                                            nativeDataType="Tag",
                                            description=f"Tag from measurement {measurement}",
                                        )
                                    )

                schema_metadata = SchemaMetadataClass(
                    schemaName=f"{db}_schema",
                    platform=f"urn:li:dataPlatform:{self.platform}",  # carefull with this, it has to be the URN of the dataset, very confusing.
                    version=0,
                    hash="",
                    platformSchema=SchemalessClass(),  # InfluxDB does not have a formal schema
                    fields=schema_fields,
                )

                yield from auto_workunit(
                    MetadataChangeProposalWrapper.construct_many(
                        entityUrn=urn,
                        aspects=[
                            DatasetPropertiesClass(
                                description=f"{measurement} from InfluxDB bucket '{db}'",
                                customProperties={
                                    "source": "InfluxDB",
                                    "measurement": measurement,
                                    "database": db,
                                },
                            ),
                            StatusClass(removed=False),
                            schema_metadata,
                            data_platform_instance,
                        ],
                    )
                )
