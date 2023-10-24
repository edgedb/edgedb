import asyncio
import urllib.parse
import random

from typing import Any, Coroutine, cast
from edb.server import tenant
from edb.server.config.types import CompositeConfigType

from . import util, ui, smtp, config


async def send_password_reset_email(
    db: Any,
    tenant: tenant.Tenant,
    to_addr: str,
    reset_url: str,
    test_mode: bool,
):
    from_addr = util.get_config(db, "ext::auth::SMTPConfig::sender")
    ui_config = cast(config.UIConfig, util.maybe_get_config(
        db, "ext::auth::AuthConfig::ui", CompositeConfigType
    ))
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
    provider: str,
    test_mode: bool,
):
    from_addr = util.get_config(db, "ext::auth::SMTPConfig::sender")
    ui_config = cast(config.UIConfig, util.maybe_get_config(
        db, "ext::auth::AuthConfig::ui", CompositeConfigType
    ))
    verification_token_params = urllib.parse.urlencode(
        {
            "verification_token": verification_token,
            "provider": provider,
            "email": to_addr,
        }
    )
    verify_url = f"{verify_url}?{verification_token_params}"
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


async def send_fake_email(tenant: tenant.Tenant):
    async def noop_coroutine():
        pass
    coro = noop_coroutine()
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
