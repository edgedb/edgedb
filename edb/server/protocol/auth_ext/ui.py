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

import html

from .util import get_config_typename


oauth_provider_names = {
  'github': 'Github',
  'google': 'Google',
  'apple': 'Apple',
  'azure': 'Azure'
}

def render_login_page(*,
    base_path: str,
    providers: frozenset,
    error_message: str = None,
    handle: str = None,
    # config
    redirect_to: str,
    app_name: str = None,
    logo_url: str = None,
    dark_logo_url: str = None,
    brand_color: str = None
):
    password_provider = [
        p for p in providers
        if get_config_typename(p) == 'ext::auth::PasswordClientConfig'
    ]
    assert(len(password_provider) <= 1)
    password_provider = (
        password_provider[0] if len(password_provider) > 0
        else None
    )

    oauth_providers = [
        p for p in providers
        if get_config_typename(p) == 'ext::auth::OAuthClientConfig'
    ]

    oauth_buttons = '\n'.join([
            f'''
            <a href="authorize?provider={p.provider_id}">
            <img src="_static/icon_{p.provider_name}.svg" alt="{
                oauth_provider_names[p.provider_name]} Icon" />
            <span>Sign in with {oauth_provider_names[p.provider_name]}</span>
            </a>'''
            for p in oauth_providers
        ])

    return render_base_page(
        title=f'Sign in{f" to {app_name}" if app_name else ""}',
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error', 'handle'],
        content=f'''
    <form method="POST" action="authenticate">
      <h1>{f'<span>Sign in to</span> {html.escape(app_name)}'
           if app_name else '<span>Sign in</span>'}</h1>

    {
      f"""
      <div class="oauth-buttons{' extended' if password_provider is None else ''}">
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
        password_provider.provider_id}" />
      <input type="hidden" name="redirect_on_failure" value="{
        base_path}/login" />
      <input type="hidden" name="redirect_to" value="{redirect_to}" />

      {render_error_message(error_message)}

      <label for="username">Username</label>
      <input id="username" name="handle" type="text" value="{handle or ''}" />

      <div class="field-header">
        <label for="password">Password</label>
        <a class="field-note" href="forgot-password">Forgot password?</a>
      </div>
      <input id="password" name="password" type="password" />

      {render_button('Sign In')}

      <div class="bottom-note">
        Don't have an account?
        <a href="signup">Sign up</a>
      </div>""" if password_provider is not None else ''
    }
    </form>'''
    )

def render_signup_page(*,
    base_path: str,
    provider_id: str,
    error_message: str = None,
    handle: str = None,
    email: str = None,
    # config
    redirect_to: str,
    app_name: str = None,
    logo_url: str = None,
    dark_logo_url: str = None,
    brand_color: str = None
):
    return render_base_page(
        title=f'Sign up{f" to {app_name}" if app_name else ""}',
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error', 'handle', 'email'],
        content=f'''
    <form method="POST" action="register">
      <h1>{f'<span>Sign up to</span> {html.escape(app_name)}'
           if app_name else '<span>Sign up</span>'}</h1>

      {render_error_message(error_message)}

      <input type="hidden" name="provider" value="{provider_id}" />
      <input type="hidden" name="redirect_on_failure" value="{
        base_path}/signup" />
      <input type="hidden" name="redirect_to" value="{redirect_to}" />

      <label for="email">Email</label>
      <input id="email" name="email" type="text" value="{email or ''}" />

      <label for="username">Username</label>
      <input id="username" name="handle" type="text" value="{handle or ''}" />

      <label for="password">Password</label>
      <input id="password" name="password" type="password" />

      {render_button('Sign Up')}

      <div class="bottom-note">
        Already have an account?
        <a href="login">Sign in</a>
      </div>
    </form>'''
    )

def render_forgot_password_page(*,
    base_path: str,
    provider_id: str,
    error_message: str = None,
    handle: str = None,
    email: str = None,
    # config
    app_name: str = None,
    logo_url: str = None,
    dark_logo_url: str = None,
    brand_color: str = None
):
    if email is not None:
        content = f'''Password reset email has been sent to {email}'''
    else:
        content = f'''
        {render_error_message(error_message)}

        <input type="hidden" name="provider" value="{provider_id}" />
        <input type="hidden" name="redirect_on_failure" value="{
          base_path}/forgot-password" />
        <input type="hidden" name="redirect_to" value="{
          base_path}/forgot-password" />
        <input type="hidden" name="reset_url" value="{
            base_path}/reset-password" />

        <label for="username">Username</label>
        <input id="username" name="handle" type="text" value="{handle or ''}" />

        {render_button('Send Reset Email')}'''

    return render_base_page(
        title=f'Reset password{f" for {app_name}" if app_name else ""}',
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error', 'handle', 'email'],
        content=f'''
    <form method="POST" action="send_reset_email">
      <h1>{f'<span>Reset password for</span> {html.escape(app_name)}'
           if app_name else '<span>Reset password</span>'}</h1>

      {content}

      <div class="bottom-note">
        Back to
        <a href="login">Sign In</a>
      </div>
    </form>'''
    )

def render_reset_password_page(*,
    base_path: str,
    provider_id: str,
    is_valid: bool,
    redirect_to: str,
    reset_token: str = None,
    error_message: str = None,
    # config
    app_name: str = None,
    logo_url: str = None,
    dark_logo_url: str = None,
    brand_color: str = None
):
    if not is_valid:
        content = f'''Reset token is invalid, it may have expired.
        <a href="forgot-password">Try sending another reset email</a>'''
    else:
        content = f'''
        {render_error_message(error_message)}

        <input type="hidden" name="provider" value="{provider_id}" />
        <input type="hidden" name="reset_token" value="{reset_token}" />
        <input type="hidden" name="redirect_on_failure" value="{
          base_path}/reset-password" />
        <input type="hidden" name="redirect_to" value="{redirect_to}" />

        <label for="password">New Password</label>
        <input id="password" name="password" type="password" />

        {render_button('Sign In')}'''

    return render_base_page(
        title=f'Reset password{f" for {app_name}" if app_name else ""}',
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error'],
        content=f'''
    <form method="POST" action="reset_password">
      <h1>{f'<span>Reset password for</span> {html.escape(app_name)}'
           if app_name else '<span>Reset password</span>'}</h1>

      {content}
    </form>'''
    )

def render_base_page(*,
    content: str,
    title: str,
    cleanup_search_params: list[str],
    logo_url: str = None,
    dark_logo_url: str = None,
    brand_color: str = None
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
  <body {'style="--brand-color: '+html.escape(brand_color)+'"'
          if brand_color else ''}>
    {logo}
    {content}
  </body>
</html>
'''.encode()

def render_error_message(error_message: str):
    return (f'''
        <div class="error-message">
        <svg xmlns="http://www.w3.org/2000/svg" width="24" height="20" viewBox="0 0 24 20" fill="none">
            <path d="M12 15H12.01M12 7.00002V11M10.29 1.86002L1.82002 16C1.64539 16.3024 1.55299 16.6453 1.55201 16.9945C1.55103 17.3438 1.64151 17.6872 1.81445 17.9905C1.98738 18.2939 2.23675 18.5468 2.53773 18.7239C2.83871 18.901 3.18082 18.9962 3.53002 19H20.47C20.8192 18.9962 21.1613 18.901 21.4623 18.7239C21.7633 18.5468 22.0127 18.2939 22.1856 17.9905C22.3585 17.6872 22.449 17.3438 22.448 16.9945C22.4471 16.6453 22.3547 16.3024 22.18 16L13.71 1.86002C13.5318 1.56613 13.2807 1.32314 12.9812 1.15451C12.6817 0.98587 12.3438 0.897278 12 0.897278C11.6563 0.897278 11.3184 0.98587 11.0188 1.15451C10.7193 1.32314 10.4683 1.56613 10.29 1.86002Z" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
        {html.escape(error_message)}
        </div>'''
        if error_message else '')

def render_button(text: str):
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
