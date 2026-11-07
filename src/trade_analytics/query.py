from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field, field_validator


class QueryType(StrEnum):
    WORLD_TOTAL = "world_total"
    PARTNER_DETAIL = "partner_detail"


class ComtradeQuery(BaseModel):
    period: str
    cmd_code: str
    reporter_code: int = Field(gt=0)
    query_type: QueryType

    @field_validator("period")
    @classmethod
    def validate_period(cls, value: str) -> str:
        if len(value) != 6 or not value.isascii() or not value.isdigit():
            raise ValueError("period must use YYYYMM format")

        try:
            datetime.strptime(value, "%Y%m")
        except ValueError as exc:
            raise ValueError("period must be a valid month") from exc

        return value

    @field_validator("cmd_code")
    @classmethod
    def validate_cmd_code(cls, value: str) -> str:
        if len(value) != 6 or not value.isascii() or not value.isdigit():
            raise ValueError("cmd_code must be a 6-digit HS code")
        return value

    def to_params(self) -> dict[str, str | int]:
        params = {
            "period": self.period,
            "reporterCode": str(self.reporter_code),
            "flowCode": "M",
            "cmdCode": self.cmd_code,
            "partner2Code": 0,
            "customsCode": "C00",
            "motCode": 0,
      }

        if self.query_type == QueryType.WORLD_TOTAL:
            params["partnerCode"] = 0

        return params
