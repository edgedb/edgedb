#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from typing import *

import html
from email.mime import multipart
from email.mime import text as mime_text


known_oauth_provider_names = [
    'builtin::oauth_github',
    'builtin::oauth_google',
    'builtin::oauth_apple',
    'builtin::oauth_azure',
]


def render_login_page(
    *,
    base_path: str,
    providers: frozenset,
    error_message: Optional[str] = None,
    email: Optional[str] = None,
    challenge: str,
    # config
    redirect_to: str,
    app_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None
):
    password_provider = None
    for p in providers:
        if p.name == 'builtin::local_emailpassword':
            password_provider = p
            break

    oauth_providers = [
        p for p in providers
        if p.name.startswith('builtin::oauth_')
    ]

    oauth_buttons = '\n'.join([
        f'''
        <a href="../authorize?provider={
            p.name
        }&redirect_to={
            redirect_to
        }&challenge={
            challenge
        }">
        {(
            '<img src="_static/icon_' + p.name[15:] + '.svg" alt="' +
            p.display_name+' Icon" />'
        ) if p.name in known_oauth_provider_names else ''}
        <span>Sign in with {p.display_name}</span>
        </a>'''
        for p in oauth_providers
    ])

    forgot_link_script = f'''<script>
        const forgotLink = document.getElementById("forgot-password-link");
        const emailInput = document.getElementById("email");

        emailInput.addEventListener("input", (e) => {{
        forgotLink.href = `forgot-password?email=${{
            encodeURIComponent(e.target.value)
        }}`
        }});
        forgotLink.href = `forgot-password?email=${{
        encodeURIComponent(emailInput.value)
        }}`
        </script>''' if password_provider is not None else ''

    return _render_base_page(
        title=f'Sign in{f" to {app_name}" if app_name else ""}',
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error', 'email'],
        content=f'''
    <form method="POST" action="../authenticate" novalidate>
      <h1>{f'<span>Sign in to</span> {html.escape(app_name)}'
           if app_name else '<span>Sign in</span>'}</h1>

    {
      f"""
      <div class="oauth-buttons">
        {oauth_buttons}
      </div>""" if len(oauth_providers) > 0 else ''
    }
    {
      """
      <div class="divider">
        <span>or</span>
      </div>"""
      if password_provider is not None
        and len(oauth_providers) > 0
      else ''
    }
    {
      f"""
      <input type="hidden" name="provider" value="{
        password_provider.name}" />
      <input type="hidden" name="redirect_on_failure" value="{
        base_path}/ui/signin" />
      <input type="hidden" name="redirect_to" value="{redirect_to}" />
      <input type="hidden" name="challenge" value="{challenge}" />

      {_render_error_message(error_message)}

      <label for="email">Email</label>
      <input id="email" name="email" type="email" value="{email or ''}" />

      <div class="field-header">
        <label for="password">Password</label>
        <a id="forgot-password-link" class="field-note" href="forgot-password">
          Forgot password?
        </a>
      </div>
      <input id="password" name="password" type="password" />

      {_render_button('Sign In')}

      <div class="bottom-note">
        Don't have an account?
        <a href="signup">Sign up</a>
      </div>""" if password_provider is not None else ''
    }
    </form>
    {forgot_link_script}'''
    )


def render_signup_page(
    *,
    base_path: str,
    provider_name: str,
    error_message: Optional[str] = None,
    email: Optional[str] = None,
    challenge: str,
    # config
    redirect_to: str,
    app_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None
):
    return _render_base_page(
        title=f'Sign up{f" to {app_name}" if app_name else ""}',
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error', 'email'],
        content=f'''
    <form method="POST" action="../register" novalidate>
      <h1>{f'<span>Sign up to</span> {html.escape(app_name)}'
           if app_name else '<span>Sign up</span>'}</h1>

      {_render_error_message(error_message)}

      <input type="hidden" name="provider" value="{provider_name}" />
      <input type="hidden" name="redirect_on_failure" value="{
        base_path}/ui/signup" />
      <input type="hidden" name="redirect_to" value="{redirect_to}" />
      <input type="hidden" name="challenge" value="{challenge}" />

      <label for="email">Email</label>
      <input id="email" name="email" type="email" value="{email or ''}" />

      <label for="password">Password</label>
      <input id="password" name="password" type="password" />

      {_render_button('Sign Up')}

      <div class="bottom-note">
        Already have an account?
        <a href="signin">Sign in</a>
      </div>
    </form>'''
    )


def render_forgot_password_page(
    *,
    base_path: str,
    provider_name: str,
    error_message: Optional[str] = None,
    email: Optional[str] = None,
    email_sent: Optional[str] = None,
    # config
    app_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None
):
    if email_sent is not None:
        content = _render_success_message(
            f'Password reset email has been sent to <b>{email_sent}</b>'
        )
    else:
        content = f'''
        {_render_error_message(error_message)}

        <input type="hidden" name="provider" value="{provider_name}" />
        <input type="hidden" name="redirect_on_failure" value="{
          base_path}/ui/forgot-password" />
        <input type="hidden" name="redirect_to" value="{
          base_path}/ui/forgot-password" />
        <input type="hidden" name="reset_url" value="{
            base_path}/ui/reset-password" />

        <label for="email">Email</label>
        <input id="email" name="email" type="email" value="{email or ''}" />

        {_render_button('Send Reset Email')}'''

    return _render_base_page(
        title=f'Reset password{f" for {app_name}" if app_name else ""}',
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error', 'email', 'email_sent'],
        content=f'''
    <form method="POST" action="../send_reset_email">
      <h1>{f'<span>Reset password for</span> {html.escape(app_name)}'
           if app_name else '<span>Reset password</span>'}</h1>

      {content}

      <div class="bottom-note">
        Back to
        <a href="signin">Sign In</a>
      </div>
    </form>'''
    )


def render_reset_password_page(
    *,
    base_path: str,
    provider_name: str,
    is_valid: bool,
    redirect_to: str,
    reset_token: Optional[str] = None,
    error_message: Optional[str] = None,
    # config
    app_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None
):
    if not is_valid:
        content = _render_error_message(
            f'''Reset token is invalid, it may have expired.
            <a href="forgot-password">Try sending another reset email</a>''',
            False
        )
    else:
        content = f'''
        {_render_error_message(error_message)}

        <input type="hidden" name="provider" value="{provider_name}" />
        <input type="hidden" name="reset_token" value="{reset_token}" />
        <input type="hidden" name="redirect_on_failure" value="{
          base_path}/ui/reset-password" />
        <input type="hidden" name="redirect_to" value="{redirect_to}" />

        <label for="password">New Password</label>
        <input id="password" name="password" type="password" />

        {_render_button('Sign In')}'''

    return _render_base_page(
        title=f'Reset password{f" for {app_name}" if app_name else ""}',
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error'],
        content=f'''
    <form method="POST" action="../reset_password">
      <h1>{f'<span>Reset password for</span> {html.escape(app_name)}'
           if app_name else '<span>Reset password</span>'}</h1>

      {content}
    </form>'''
    )


def _render_base_page(
    *,
    content: str,
    title: str,
    cleanup_search_params: list[str],
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None,
):
    logo = f'''
      <picture class="brand-logo">
        {'<source srcset="'+html.escape(dark_logo_url)+
          '" media="(prefers-color-scheme: dark)" />'
          if dark_logo_url else ''}
        <img src="{html.escape(logo_url)}" />
      </picture>''' if logo_url else ''

    cleanup_script = f'''<script>
      const params = ["{'", "'.join(cleanup_search_params)}"];
      const url = new URL(location);
      if (params.some((p) => url.searchParams.has(p))) {{
        for (const p of params) {{
          url.searchParams.delete(p);
        }}
        history.replaceState(null, '', url);
      }}
    </script>''' if len(cleanup_search_params) > 0 else ''

    return f'''
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width" />
    <link rel="stylesheet" href="_static/styles.css" />
    <title>{html.escape(title)}</title>
    {cleanup_script}
  </head>
  <body {'style="'+get_colour_vars(brand_color or '1f8aed')+'"'}>
    {logo}
    {content}
  </body>
</html>
'''.encode()


def _render_error_message(error_message: Optional[str], escape: bool = True):
    return (f'''
        <div class="error-message">
        <svg xmlns="http://www.w3.org/2000/svg" width="24" height="20"
          viewBox="0 0 24 20" fill="none">
            <path d="M12 15H12.01M12 7.00002V11M10.29 1.86002L1.82002
              16C1.64539 16.3024 1.55299 16.6453 1.55201 16.9945C1.55103
              17.3438 1.64151 17.6872 1.81445 17.9905C1.98738 18.2939 2.23675
              18.5468 2.53773 18.7239C2.83871 18.901 3.18082 18.9962 3.53002
              19H20.47C20.8192 18.9962 21.1613 18.901 21.4623 18.7239C21.7633
              18.5468 22.0127 18.2939 22.1856 17.9905C22.3585 17.6872 22.449
              17.3438 22.448 16.9945C22.4471 16.6453 22.3547 16.3024 22.18
              16L13.71 1.86002C13.5318 1.56613 13.2807 1.32314 12.9812
              1.15451C12.6817 0.98587 12.3438 0.897278 12 0.897278C11.6563
              0.897278 11.3184 0.98587 11.0188 1.15451C10.7193 1.32314 10.4683
              1.56613 10.29 1.86002Z"
              stroke="currentColor" stroke-width="1.5" stroke-linecap="round"
              stroke-linejoin="round"/>
        </svg>
        <span>{html.escape(error_message) if escape else error_message}</span>
        </div>'''
            if error_message else '')


def _render_success_message(success_message: str):
    return f'''
        <div class="success-message">
        <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24"
          viewBox="0 0 24 24" fill="none">
          <path d="M22 2L11 13" stroke="currentColor" stroke-width="1.5"
            stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M22 2L15 22L11 13L2 9L22 2Z" stroke="currentColor"
            stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
        <span>{success_message}</span>
        </div>'''


def _render_button(text: str):
    return f'''
    <button type="submit">
        <span>{text}</span>
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="24"
          height="25"
          viewBox="0 0 24 25"
          fill="none"
        >
          <path
            d="M5 12.5H19"
            stroke="currentColor"
            stroke-width="1.75"
            stroke-linecap="round"
            stroke-linejoin="round"
          />
          <path
            d="M12 5.5L19 12.5L12 19.5"
            stroke="currentColor"
            stroke-width="1.75"
            stroke-linecap="round"
            stroke-linejoin="round"
          />
        </svg>
      </button>'''


def render_password_reset_email(
    *,
    from_addr: str,
    to_addr: str,
    reset_url: str,
    app_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None,
) -> multipart.MIMEMultipart:
    msg = multipart.MIMEMultipart()
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = "Reset password"
    alternative = multipart.MIMEMultipart('alternative')
    plain_text_msg = mime_text.MIMEText(
        f"""
        {reset_url}
        """,
        "plain",
        "utf-8",
    )
    alternative.attach(plain_text_msg)
    html_msg = mime_text.MIMEText(
        f"""
<!DOCTYPE html>
<html>
  <body>
    <a href="{reset_url}">Reset password</a>
  </body>
</html>
        """,
        "html",
        "utf-8",
    )
    alternative.attach(html_msg)
    msg.attach(alternative)
    return msg


# Colour utils


def get_colour_vars(bg_hex: str):
    bg_rgb = hex_to_rgb(bg_hex)
    bg_hsl = rgb_to_hsl(*bg_rgb)
    luma = rgb_to_luma(*bg_rgb)
    luma_dark = luma < 0.6

    return f'''--accent-bg-color: #{bg_hex};
        --accent-bg-text-color: #{rgb_to_hex(
            *hsl_to_rgb(
                bg_hsl[0],
                bg_hsl[1],
                95 if luma_dark else max(10, min(25, luma * 100 - 60))
            )
        )};
        --accent-bg-hover-color: #{rgb_to_hex(
            *hsl_to_rgb(
                bg_hsl[0], bg_hsl[1], bg_hsl[2] + (5 if luma_dark else -5)
            )
        )};
        --accent-text-color: #{rgb_to_hex(
            *hsl_to_rgb(
                bg_hsl[0], bg_hsl[1], min(90 if luma_dark else 35, bg_hsl[2])
            )
        )};
        --accent-text-dark-color: #{rgb_to_hex(
            *hsl_to_rgb(bg_hsl[0], bg_hsl[1], max(60, bg_hsl[2]))
        )}'''


def hex_to_rgb(hex: str) -> tuple[float, float, float]:
    return (
        int(hex[0:2], base=16),
        int(hex[2:4], base=16),
        int(hex[4:6], base=16),
    )


def rgb_to_hex(r: float, g: float, b: float) -> str:
    return '%02x%02x%02x' % (int(r), int(g), int(b))


def rgb_to_luma(r: float, g: float, b: float) -> float:
    return (r * 0.299 + g * 0.587 + b * 0.114) / 255


def rgb_to_hsl(r: float, g: float, b: float) -> tuple[float, float, float]:
    r /= 255
    g /= 255
    b /= 255
    l = max(r, g, b)
    s = l - min(r, g, b)
    h = (
        ((g - b) / s) if l == r else
        (2 + (b - r) / s) if l == g else
        (4 + (r - g) / s)
    ) if s != 0 else 0
    return (
        60 * h + 360 if 60 * h < 0 else 60 * h,
        100 * (
            (s / (2 * l - s) if l <= 0.5 else s / (2 - (2 * l - s)))
            if s != 0 else 0
        ),
        (100 * (2 * l - s)) / 2,
    )


def hsl_to_rgb(h: float, s: float, l: float) -> tuple[float, float, float]:
    s /= 100
    l /= 100
    k = lambda n: (n + h / 30) % 12
    a = s * min(l, 1 - l)
    f = lambda n: l - a * max(-1, min(k(n) - 3, min(9 - k(n), 1)))
    return (
        round(255 * f(0)),
        round(255 * f(8)),
        round(255 * f(4)),
    )
