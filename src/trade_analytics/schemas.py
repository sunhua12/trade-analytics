import logging

from decimal import Decimal

from pydantic import BaseModel, Field, ConfigDict, field_validator


logger = logging.getLogger(__name__)


class TradeRecord(BaseModel):
    model_config = ConfigDict(extra="allow", strict=True)

    period: str
    reporter_code: int = Field(alias="reporterCode")
    partner_code: int = Field(alias="partnerCode", ge=0)
    flow_code: str = Field(alias="flowCode")
    cmd_code: str = Field(alias="cmdCode")
    primary_value: Decimal = Field(alias="primaryValue", ge=0, allow_inf_nan=False)
    classification_code: str = Field(alias="classificationCode")
    freq_code: str = Field(alias="freqCode")
    partner2_code: int = Field(alias="partner2Code")
    customs_code: str = Field(alias="customsCode")
    mot_code: int = Field(alias="motCode")
    is_original_classification: bool = Field(alias="isOriginalClassification")

    @field_validator("primary_value", mode="before")
    @classmethod
    def validate_primary_value(cls, value: object) -> Decimal:
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            logger.debug("primaryValue 型別驗證失敗：%s", type(value).__name__)
            raise ValueError("primaryValue 必須是數字，且不可為 bool")

        return Decimal(str(value))


class ComtradeResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    data: list[TradeRecord]
