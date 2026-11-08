import logging

import decimal

import pydantic


logger = logging.getLogger(__name__)


class TradeRecord(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="allow", strict=True)

    period: str
    reporter_code: int = pydantic.Field(alias="reporterCode")
    partner_code: int = pydantic.Field(alias="partnerCode", ge=0)
    flow_code: str = pydantic.Field(alias="flowCode")
    cmd_code: str = pydantic.Field(alias="cmdCode")
    primary_value: decimal.Decimal = pydantic.Field(alias="primaryValue", ge=0, allow_inf_nan=False)
    classification_code: str = pydantic.Field(alias="classificationCode")
    freq_code: str = pydantic.Field(alias="freqCode")
    partner2_code: int = pydantic.Field(alias="partner2Code")
    customs_code: str = pydantic.Field(alias="customsCode")
    mot_code: int = pydantic.Field(alias="motCode")
    is_original_classification: bool = pydantic.Field(alias="isOriginalClassification")

    @pydantic.field_validator("primary_value", mode="before")
    @classmethod
    def validate_primary_value(cls, value: object) -> decimal.Decimal:
        if isinstance(value, bool) or not isinstance(value, (int, float, decimal.Decimal)):
            logger.debug("primaryValue 型別驗證失敗：%s", type(value).__name__)
            raise ValueError("primaryValue 必須是數字，且不可為 bool")

        return decimal.Decimal(str(value))


class ComtradeResponse(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="allow")

    data: list[TradeRecord]
