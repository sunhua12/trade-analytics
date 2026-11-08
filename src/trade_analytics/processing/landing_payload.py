import logging
import dataclasses
import typing

from trade_analytics.ingestion import batch
from trade_analytics.contracts import data_contract
from trade_analytics.contracts import query_identity

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class LandingPayload:
    batch_id: str
    source_query_key: str
    query_type: str
    records: list[dict[str, typing.Any]]

    @property
    def record_count(self) -> int:
        return len(self.records)


def build_landing_payload(result: batch.BatchResult, batch_id: str) -> LandingPayload:
    """驗證單組 BatchResult，建立符合 Landing Schema 的 Payload。"""
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("batch_id 不可為空")

    query_identity.validate_record_uniqueness(result)

    unmodeled_fields = query_identity.find_unmodeled_fields(result)
    if unmodeled_fields:
        logger.warning("發現尚未分類的來源欄位：%s", sorted(unmodeled_fields), )

    source_query_key = query_identity.build_source_query_key(result.query)
    records = data_contract.to_landing_records(result=result, batch_id=batch_id, source_query_key=source_query_key, )
    payload = LandingPayload(batch_id=batch_id, source_query_key=source_query_key,
                             query_type=result.query.query_type.value, records=records, )

    logger.info(
        "Landing Payload 建立成功：batch_id=%s query_key=%s records=%d",
        payload.batch_id,
        payload.source_query_key,
        payload.record_count,
    )

    return payload
