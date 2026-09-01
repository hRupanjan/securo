import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_async_session
from app.core.workspace_context import (
    WorkspaceContext,
    current_workspace,
    current_writable_workspace,
)
from app.schemas.transaction import TransactionImportPreview, TransactionImportRequest
from app.services import account_service, import_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/transactions", tags=["import"])


@router.post("/import/preview", response_model=TransactionImportPreview)
async def preview_import(
    file: UploadFile = File(...),
    date_format: Optional[str] = Form(None),
    flip_amount: bool = Form(False),
    inflow_column: Optional[str] = Form(None),
    outflow_column: Optional[str] = Form(None),
    column_mapping: Optional[str] = Form(None),
    # Read-gated on purpose, and the exception is deliberate rather than an
    # oversight. This is a POST because it takes a file upload, not because
    # it changes anything: it parses the upload and returns what *would* be
    # imported. The only workspace data it touches is
    # `enrich_with_category_suggestions`, which SELECTs rules and categories
    # to label the preview. Nothing is persisted, so a read-only member
    # previewing a file is doing exactly what their role allows. The write
    # gate belongs on `POST /import` below, which is where the rows land.
    ctx: WorkspaceContext = Depends(current_workspace),
    session: AsyncSession = Depends(get_async_session),
):
    content = await file.read()
    filename = file.filename or ""

    logger.info(
        "Import preview requested: filename=%s, size=%d bytes, content_type=%s",
        filename, len(content), file.content_type,
    )

    # column_mapping arrives as a JSON-encoded form field (Securo field -> CSV header)
    parsed_mapping: Optional[dict] = None
    if column_mapping:
        try:
            parsed_mapping = json.loads(column_mapping)
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid column_mapping: must be a JSON object",
            )
        if not isinstance(parsed_mapping, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid column_mapping: must be a JSON object",
            )

    parse_error: Optional[str] = None

    def parse_csv_with_options() -> tuple[list, Optional[str]]:
        """Parse the upload as CSV, honouring the caller's mapping options.

        Returns (transactions, parse_error). When the columns can't be
        resolved but the headers are readable, this soft-fails so the UI can
        offer the column-mapping dropdowns instead of a hard error.
        """
        try:
            return import_service.parse_csv(
                content,
                date_format=date_format,
                flip_amount=flip_amount,
                inflow_column=inflow_column,
                outflow_column=outflow_column,
                column_mapping=parsed_mapping,
            ), None
        except ValueError as csv_err:
            if not import_service.detect_csv_columns(content):
                raise
            return [], str(csv_err)

    try:
        if filename.lower().endswith('.ofx') or filename.lower().endswith('.qfx'):
            transactions = import_service.parse_ofx(content)
            detected_format = "ofx"
        elif filename.lower().endswith('.qif'):
            transactions = import_service.parse_qif(content, date_format=date_format)
            detected_format = "qif"
        elif filename.lower().endswith('.xml') or filename.lower().endswith('.camt'):
            transactions = import_service.parse_camt(content)
            detected_format = "camt"
        elif filename.lower().endswith('.csv'):
            detected_format = "csv"
            transactions, parse_error = parse_csv_with_options()
        else:
            # Try to detect format. A parser that returns no rows hasn't
            # recognized the file — parse_qif in particular accepts any text
            # and yields [] — so keep sniffing instead of reporting an empty
            # import, and let CSV (with its column mapping) have the last try.
            transactions = []
            detected_format = "csv"
            for fmt, parser in (
                ("ofx", lambda: import_service.parse_ofx(content)),
                ("qif", lambda: import_service.parse_qif(content, date_format=date_format)),
                ("camt", lambda: import_service.parse_camt(content)),
            ):
                try:
                    parsed = parser()
                except Exception:
                    continue
                if parsed:
                    transactions, detected_format = parsed, fmt
                    break
            else:
                transactions, parse_error = parse_csv_with_options()
    except Exception as e:
        logger.error(
            "Failed to parse import file: filename=%s, size=%d bytes, "
            "content_type=%s, first_100_bytes=%r, error=%s",
            filename, len(content), file.content_type,
            content[:100], e,
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to parse file: {str(e)}",
        )

    logger.info(
        "Import preview parsed: filename=%s, format=%s, transactions=%d",
        filename, detected_format, len(transactions),
    )

    transactions = await import_service.enrich_with_category_suggestions(
        session, ctx.workspace.id, transactions,
    )

    # Expose CSV headers so the UI can offer accurate column-mapping dropdowns.
    csv_columns: list[str] = []
    if detected_format == "csv":
        try:
            csv_columns = import_service.detect_csv_columns(content)
        except Exception:
            csv_columns = []

    return TransactionImportPreview(
        transactions=transactions,
        detected_format=detected_format,
        csv_columns=csv_columns,
        parse_error=parse_error,
    )


@router.post("/import", status_code=status.HTTP_201_CREATED)
async def import_transactions(
    data: TransactionImportRequest,
    ctx: WorkspaceContext = Depends(current_writable_workspace),
    session: AsyncSession = Depends(get_async_session),
):
    # Verify the target account lives in this workspace BEFORE doing any
    # writes — otherwise a hand-rolled request could import into an
    # account owned by another tenant.
    account = await account_service.get_account(session, data.account_id, ctx.workspace.id)
    if not account:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")

    imported, skipped, excluded, import_log_id = await import_service.import_transactions(
        session, ctx.workspace.id, ctx.user_id, data.account_id, data.transactions, "import",
        filename=data.filename, detected_format=data.detected_format,
        detect_duplicates=data.detect_duplicates,
    )

    return {
        "imported": imported,
        "skipped": skipped,
        "excluded": excluded,
        "import_log_id": str(import_log_id),
    }
