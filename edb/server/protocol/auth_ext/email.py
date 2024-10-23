import asyncio
import urllib.parse
import random

from typing import Any, Coroutine
from edb.server import tenant

from . import util, ui, smtp


async def send_password_reset_email(
    db: Any,
    tenant: tenant.Tenant,
    to_addr: str,
    reset_url: str,
    test_mode: bool,
) -> None:
    from_addr = util.get_config(db, "ext::auth::SMTPConfig::sender")
    app_details_config = util.get_app_details_config(db)
    if app_details_config is None:
        email_args = {}
    else:
        email_args = dict(
            app_name=app_details_config.app_name,
            logo_url=app_details_config.logo_url,
            dark_logo_url=app_details_config.dark_logo_url,
            brand_color=app_details_config.brand_color,
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
) -> None:
    from_addr = util.get_config(db, "ext::auth::SMTPConfig::sender")
    app_details_config = util.get_app_details_config(db)
    verification_token_params = urllib.parse.urlencode(
        {
            "verification_token": verification_token,
            "provider": provider,
            "email": to_addr,
        }
    )
    verify_url = f"{verify_url}?{verification_token_params}"
    if app_details_config is None:
        email_args = {}
    else:
        email_args = dict(
            app_name=app_details_config.app_name,
            logo_url=app_details_config.logo_url,
            dark_logo_url=app_details_config.dark_logo_url,
            brand_color=app_details_config.brand_color,
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


async def send_magic_link_email(
    db: Any,
    tenant: tenant.Tenant,
    to_addr: str,
    link: str,
    test_mode: bool,
) -> None:
    from_addr = util.get_config(db, "ext::auth::SMTPConfig::sender")
    app_details_config = util.get_app_details_config(db)
    if app_details_config is None:
        email_args = {}
    else:
        email_args = dict(
            app_name=app_details_config.app_name,
            logo_url=app_details_config.logo_url,
            dark_logo_url=app_details_config.dark_logo_url,
            brand_color=app_details_config.brand_color,
        )
    msg = ui.render_magic_link_email(
        from_addr=from_addr,
        to_addr=to_addr,
        link=link,
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


async def send_fake_email(tenant: tenant.Tenant) -> None:
    async def noop_coroutine() -> None:
        pass

    coro = noop_coroutine()
    await _protected_send(coro, tenant)


async def _protected_send(
    coro: Coroutine[Any, Any, None], tenant: tenant.Tenant
) -> None:
    task = tenant.create_task(coro, interruptable=True)
    # Prevent timing attack
    await asyncio.sleep(random.random() * 0.5)
    # Expose e.g. configuration errors
    if task.done():
        await task
