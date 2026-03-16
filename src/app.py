"""aiohttp entry point for the Email Triage Agent."""

from __future__ import annotations

import asyncio
import logging
import sys

from aiohttp import web
from aiohttp.web import Request, Response
from botbuilder.core import (
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
)
from botbuilder.schema import Activity

from src.background.scheduler import TriageScheduler
from src.bot import TriageBot, store_conversation_reference
from src.config import get_settings
from src.webhooks.mail_handler import process_mail_notification

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


async def _on_error(context: TurnContext, error: Exception) -> None:
    logger.error("Unhandled bot error: %s", error, exc_info=True)
    await context.send_activity("Sorry, something went wrong. Please try again later.")


def _create_adapter() -> BotFrameworkAdapter:
    s = get_settings()
    settings = BotFrameworkAdapterSettings(
        app_id=s.microsoft_app_id,
        app_password=s.microsoft_app_password,
        channel_auth_tenant=s.microsoft_app_tenant_id,
    )
    adapter = BotFrameworkAdapter(settings)
    adapter.on_turn_error = _on_error
    return adapter


async def _messages(req: Request) -> Response:
    if "application/json" not in (req.content_type or ""):
        return Response(status=415)
    body = await req.json()
    activity = Activity().deserialize(body)

    adapter: BotFrameworkAdapter = req.app["adapter"]
    bot: TriageBot = req.app["bot"]

    async def _turn_callback(turn_context: TurnContext) -> None:
        _save_ref(turn_context)
        await bot.on_turn(turn_context)

    auth_header = req.headers.get("Authorization", "")
    response = await adapter.process_activity(activity, auth_header, _turn_callback)
    if response:
        return Response(body=response.body, status=response.status)
    return Response(status=201)


def _save_ref(turn_context: TurnContext) -> None:
    ref = TurnContext.get_conversation_reference(turn_context.activity)
    from_prop = turn_context.activity.from_property
    key = getattr(from_prop, "aad_object_id", None) or getattr(from_prop, "id", None)
    if key:
        store_conversation_reference(key, ref.as_dict() if hasattr(ref, "as_dict") else ref.__dict__)


async def _notifications(req: Request) -> Response:
    """Handle Graph change notifications for new mail."""
    # Graph subscription validation handshake
    validation_token = req.query.get("validationToken")
    if validation_token:
        return Response(text=validation_token, content_type="text/plain")

    if "application/json" not in (req.content_type or ""):
        return Response(status=415)

    body = await req.json()
    notifications = body.get("value", [])
    adapter: BotFrameworkAdapter = req.app["adapter"]

    for notification in notifications:
        client_state = notification.get("clientState", "")
        if client_state != "email-triage-agent":
            continue

        change_type = notification.get("changeType", "")
        if change_type == "created":
            asyncio.create_task(_safe_process(notification, adapter))

    return Response(status=202)


async def _safe_process(notification: dict, adapter: BotFrameworkAdapter) -> None:
    try:
        await process_mail_notification(notification, adapter)
    except Exception:
        logger.error("Failed to process mail notification.", exc_info=True)


async def _health(req: Request) -> Response:
    return Response(text="OK")


async def init_app() -> web.Application:
    app = web.Application()
    adapter = _create_adapter()
    bot = TriageBot()
    scheduler = TriageScheduler(adapter)

    app["adapter"] = adapter
    app["bot"] = bot
    app["scheduler"] = scheduler

    app.router.add_post("/api/messages", _messages)
    app.router.add_post("/api/notifications", _notifications)
    app.router.add_get("/health", _health)

    async def on_startup(_app: web.Application) -> None:
        try:
            await scheduler.start()
        except Exception:
            logger.error("Scheduler start failed -- will retry on config reload.", exc_info=True)

    app.on_startup.append(on_startup)
    return app


if __name__ == "__main__":
    web.run_app(init_app(), port=get_settings().port)
