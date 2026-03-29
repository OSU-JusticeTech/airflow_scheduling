import json
import os
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

# Bootstrap Django when called from Airflow or CLI.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django  # noqa: E402

django.setup()

from django.db import connection, transaction  # noqa: E402

from courtdata_django.models import (  # noqa: E402
    Address,
    Attorney,
    Cases,
    Disposition,
    Docket,
    Events,
    Party,
)


DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%b %d %Y",
    "%B %d %Y",
    "%b %d, %Y",
    "%B %d, %Y",
)


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_date(value: Any):
    text = _as_str(value)
    if not text:
        return None

    upper_text = text.upper().replace("  ", " ")
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(upper_text, fmt).date()
        except ValueError:
            continue

    return None


def _parse_decimal(value: Any):
    text = _as_str(value)
    if not text:
        return None

    normalized = text.replace("$", "").replace(",", "")
    try:
        return Decimal(normalized)
    except (InvalidOperation, ValueError):
        return None


def _ensure_dict(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _ensure_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _split_state_zip(value: Any) -> tuple[str | None, str | None]:
    text = _as_str(value)
    if not text:
        return None, None

    if "/" in text:
        left, right = text.split("/", 1)
        return _as_str(left), _as_str(right)

    parts = text.split()
    if len(parts) >= 2:
        return _as_str(parts[0]), _as_str(parts[-1])

    return _as_str(text), None


def _create_address(addr_like: Any, addr_type: str | None = None) -> Address | None:
    addr = _ensure_dict(addr_like)

    # Supports parser payloads where address is plain text.
    if not addr:
        line1 = _as_str(addr_like)
        if not line1:
            return None
        return Address.objects.create(
            address_type=_as_str(addr_type),
            address_line1=line1,
            address_line2=None,
            city=None,
            state=None,
            country="USA",
            postal_code=None,
        )

    state, postal = _split_state_zip(addr.get("state_zip"))

    return Address.objects.create(
        address_type=_as_str(addr_type) or _as_str(addr.get("address_type")),
        address_line1=_as_str(addr.get("address_line1") or addr.get("address")),
        address_line2=_as_str(addr.get("address_line2")),
        city=_as_str(addr.get("city")),
        state=_as_str(addr.get("state")) or state,
        country=_as_str(addr.get("country")) or "USA",
        postal_code=_as_str(addr.get("postal_code")) or postal,
    )


def _link_case_party(case_id: int, party_id: int) -> None:
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO case_party (case_id, party_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            [case_id, party_id],
        )


@transaction.atomic
def load_case_payload(data: dict[str, Any], source_id: str | None = None, dag_run_id: str | None = None, task_id: str | None = None) -> dict[str, Any]:
    case_number = _as_str(data.get("case_number")) or _as_str(source_id) or "unknown"
    case_filed_date = _parse_date(data.get("case_filed_date"))

    case_obj, _created = Cases.objects.get_or_create(
        case_number=case_number,
        defaults={
            "case_title": _as_str(data.get("case_title")),
            "case_description": _as_str(data.get("case_description")),
            "case_status": _as_str(data.get("case_status")),
            "case_filed_date": case_filed_date,
            "created_dag_run_id": dag_run_id,
            "pipeline_status": "New",
        },
    )

    # Keep top-level case fields fresh while preserving DB-managed timestamps.
    case_obj.case_title = _as_str(data.get("case_title")) or case_obj.case_title
    case_obj.case_description = _as_str(data.get("case_description")) or case_obj.case_description
    case_obj.case_status = _as_str(data.get("case_status")) or case_obj.case_status
    case_obj.case_filed_date = case_filed_date or case_obj.case_filed_date
    case_obj.created_dag_run_id = dag_run_id or case_obj.created_dag_run_id
    case_obj.updated_task_id = task_id or case_obj.updated_task_id
    case_obj.pipeline_status = "Valid"
    case_obj.save()

    inserted = {
        "parties": 0,
        "attorneys": 0,
        "dispositions": 0,
        "events": 0,
        "dockets": 0,
    }

    party_rows = _ensure_list(data.get("parties"))
    for party in party_rows:
        party_addr = _create_address(party.get("address"), addr_type="party")
        party_obj = Party.objects.create(
            party_name=_as_str(party.get("party_name") or party.get("name")),
            party_type=_as_str(party.get("party_type") or party.get("type")),
            address=party_addr,
            created_dag_run_id=dag_run_id,
            updated_task_id=task_id,
        )
        _link_case_party(case_obj.case_id, party_obj.party_id)
        inserted["parties"] += 1

    attorney_rows = _ensure_list(data.get("attorneys"))
    for attorney in attorney_rows:
        atty_name = _as_str(attorney.get("attorney_name") or attorney.get("name"))
        atty_type = _as_str(attorney.get("attorney_type") or attorney.get("party_type")) or "Attorney"
        atty_addr = _create_address(attorney.get("address"), addr_type="attorney")

        atty_party = Party.objects.create(
            party_name=atty_name,
            party_type=atty_type,
            address=atty_addr,
            created_dag_run_id=dag_run_id,
            updated_task_id=task_id,
        )

        Attorney.objects.create(
            attorney_name=atty_name,
            party=atty_party,
            attorney_type=atty_type,
            address=atty_addr,
            created_dag_run_id=dag_run_id,
            updated_task_id=task_id,
        )
        inserted["attorneys"] += 1

    disposition_rows = _ensure_list(data.get("dispositions"))
    if not disposition_rows:
        single_disposition = _ensure_dict(data.get("disposition"))
        if single_disposition:
            disposition_rows = [single_disposition]
    for disp in disposition_rows:
        Disposition.objects.create(
            case=case_obj,
            disposition_status=_as_str(disp.get("disposition_status") or disp.get("status")),
            disposition_status_date=_parse_date(disp.get("disposition_status_date") or disp.get("status_date")),
            disposition_code=_as_str(disp.get("disposition_code") or disp.get("disposition_code_text")),
            disposition_date=_parse_date(disp.get("disposition_date")),
            judge=_as_str(disp.get("judge")),
            created_dag_run_id=dag_run_id,
            updated_task_id=task_id,
        )
        inserted["dispositions"] += 1

    event_rows = _ensure_list(data.get("events"))
    for event in event_rows:
        Events.objects.create(
            case=case_obj,
            event_name=_as_str(event.get("event_name") or event.get("name")),
            event_date=_parse_date(event.get("event_date") or event.get("date")),
            event_start_time=_as_str(event.get("event_start_time") or event.get("start_time")),
            event_end_time=_as_str(event.get("event_end_time") or event.get("end_time")),
            event_judge=_as_str(event.get("event_judge") or event.get("judge")),
            event_courtroom=_as_str(event.get("event_courtroom") or event.get("courtroom")),
            event_result=_as_str(event.get("event_result") or event.get("result")),
            created_dag_run_id=dag_run_id,
            updated_task_id=task_id,
        )
        inserted["events"] += 1

    docket_rows = _ensure_list(data.get("dockets") or data.get("docket"))
    for docket in docket_rows:
        Docket.objects.create(
            case=case_obj,
            docket_date=_parse_date(docket.get("docket_date") or docket.get("date")),
            docket_text=_as_str(docket.get("docket_text") or docket.get("entry")),
            docket_currency=_as_str(docket.get("docket_currency")) or "USD",
            docket_amount=_parse_decimal(docket.get("docket_amount") or docket.get("amount")),
            docket_balance=_parse_decimal(docket.get("docket_balance") or docket.get("balance")),
            created_dag_run_id=dag_run_id,
            updated_task_id=task_id,
        )
        inserted["dockets"] += 1

    return {
        "case_number": case_number,
        "case_id": case_obj.case_id,
        "inserted": inserted,
        "source": _as_str(source_id),
    }


@transaction.atomic
def load_gliner_case_json(json_path: str | Path, dag_run_id: str | None = None, task_id: str | None = None) -> dict[str, Any]:
    path = Path(json_path)
    if not path.exists():
        raise FileNotFoundError(f"GLiNER JSON not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    result = load_case_payload(data, source_id=str(path), dag_run_id=dag_run_id, task_id=task_id)
    result["json_path"] = str(path)
    return result


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Load a GLiNER JSON file into Postgres using Django ORM")
    parser.add_argument("json_path", help="Path to GLiNER JSON output file")
    parser.add_argument("--dag-run-id", default=None)
    parser.add_argument("--task-id", default=None)
    args = parser.parse_args()

    result = load_gliner_case_json(args.json_path, dag_run_id=args.dag_run_id, task_id=args.task_id)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
