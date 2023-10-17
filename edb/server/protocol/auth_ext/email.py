import asyncio
import urllib.parse
import random

from typing import Any, Coroutine
from edb.server import tenant

from edb.server.config.types import CompositeConfigType

from . import util, ui, smtp


async def send_password_reset_email(
    db: Any,
    tenant: tenant.Tenant,
    to_addr: str,
    reset_url: str,
    secret_token: str,
    test_mode: bool,
):
    from_addr = util.get_config(db, "ext::auth::SMTPConfig::sender")
    reset_token_params = urllib.parse.urlencode({"reset_token": secret_token})
    reset_url = f"{reset_url}?{reset_token_params}"
    ui_config = util.maybe_get_config(
        db, "ext::auth::AuthConfig::ui", CompositeConfigType
    )
    email_args: dict[str, str]
    if ui_config is None:
        email_args = {}
    else:
        email_args = dict(
            app_name=ui_config.app_name,
            logo_url=ui_config.logo_url,
            dark_logo_url=ui_config.dark_logo_url,
            brand_color=ui_config.brand_color,
        )
    msg = ui.render_password_reset_email(
        from_addr=from_addr,
        to_addr=to_addr,
        reset_url=reset_url,
        **email_args,
    )
    coro = smtp.send_email(
        db,
        msg,
        sender=from_addr,
        recipients=to_addr,
        test_mode=test_mode,
    )
    await _protected_send(coro, tenant)


async def send_verification_email(
    db: Any,
    tenant: tenant.Tenant,
    to_addr: str,
    verify_url: str,
    verification_token: str,
    test_mode: bool,
):
    from_addr = util.get_config(db, "ext::auth::SMTPConfig::sender")
    ui_config = util.maybe_get_config(
        db, "ext::auth::AuthConfig::ui", CompositeConfigType
    )
    verification_token_params = urllib.parse.urlencode(
        {"verification_token": verification_token}
    )
    verify_url = f"{verify_url}?{verification_token_params}"
    email_args: dict[str, str]
    if ui_config is None:
        email_args = {}
    else:
        email_args = dict(
            app_name=ui_config.app_name,
            logo_url=ui_config.logo_url,
            dark_logo_url=ui_config.dark_logo_url,
            brand_color=ui_config.brand_color,
        )
    msg = ui.render_verification_email(
        from_addr=from_addr,
        to_addr=to_addr,
        verify_url=verify_url,
        **email_args,
    )
    coro = smtp.send_email(
        db,
        msg,
        sender=from_addr,
        recipients=to_addr,
        test_mode=test_mode,
    )
    await _protected_send(coro, tenant)


async def _protected_send(
    coro: Coroutine[Any, Any, None], tenant: tenant.Tenant
):
    task = tenant.create_task(coro, interruptable=False)
    # Prevent timing attack
    await asyncio.sleep(random.random() * 0.5)
    # Expose e.g. configuration errors
    if task.done():
        await task
