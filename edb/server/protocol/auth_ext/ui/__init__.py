# flake8: noqa: e126, e121, e127, e128
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

from . import components as render


def render_signin_page(
    *,
    base_path: str,
    providers: frozenset,
    error_message: Optional[str] = None,
    email: Optional[str] = None,
    challenge: str,
    selected_tab: Optional[str] = None,
    # config
    redirect_to: str,
    redirect_to_on_signup: Optional[str] = None,
    app_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None,
):
    password_provider = None
    webauthn_provider = None
    magic_link_provider = None
    oauth_providers = []
    for p in providers:
        if p.name == 'builtin::local_emailpassword':
            password_provider = p
        elif p.name == 'builtin::local_webauthn':
            webauthn_provider = p
        elif p.name == 'builtin::local_magic_link':
            magic_link_provider = p
        elif p.name.startswith('builtin::oauth_'):
            oauth_providers.append(p)

    base_email_factor_form = f"""
      <input type="hidden" name="challenge" value="{challenge}" />

      <label for="email">Email</label>
      <input id="email" name="email" type="email" value="{email or ''}" />
    """

    password_input = f"""
        <div class="field-header">
          <label for="password">Password</label>
          <a
            id="forgot-password-link"
            class="field-note"
            href="forgot-password?challenge={challenge}"
            tabindex="-1">
            Forgot password?
          </a>
        </div>
        <input id="password" name="password" type="password" />
    """ if password_provider else ''

    email_factor_form = render_email_factor_form(
        base_email_factor_form=base_email_factor_form,
        password_input=password_input,
        selected_tab=selected_tab,
        single_form_fields=f'''
            {render.hidden_input(
                name='redirect_to',
                value=(
                    redirect_to if webauthn_provider
                    else (base_path + '/ui/magic-link-sent')
                ),
                secondary_value=redirect_to
            )}
            {render.hidden_input(
                name='redirect_on_failure',
                value=f'{base_path}/ui/signin',
                secondary_value=f'{base_path}/ui/signin?selected_tab=password'
            )}
            {render.hidden_input(
                name='provider',
                value=magic_link_provider.name if magic_link_provider else '',
                secondary_value=(
                    password_provider.name if password_provider else '')
            )}
            {render.hidden_input(
                name='callback_url', value=redirect_to
            ) if magic_link_provider else ''}
        ''',
        password_form=f"""
            <form
                method="post"
                action="../authenticate"
                novalidate
            >
                <input type="hidden" name="redirect_to" value="{
                    redirect_to}" />
                <input type="hidden" name="redirect_on_failure" value="{
                    base_path}/ui/signin?selected_tab=password" />
                <input type="hidden" name="provider" value="{
                    password_provider.name}" />
                {base_email_factor_form}
                {password_input}
                {render.button("Sign In", id="password-signin")}
            </form>
        """ if password_provider else None,
        webauthn_form=f"""
            <form
                id="email-factor"
                novalidate
            >
                <input type="hidden" name="redirect_to" value="{
                    redirect_to}" />
                <input type="hidden" name="redirect_on_failure" value="{
                    base_path}/ui/signin?selected_tab=webauthn" />
                {base_email_factor_form}
                {render.button("Sign In", id="webauthn-signin")}
            </form>
        """ if webauthn_provider else None,
        magic_link_form=f"""
            <form
                method="post"
                action="../magic-link/email"
                novalidate
            >
                <input type="hidden" name="redirect_to" value="{
                    base_path}/ui/magic-link-sent" />
                <input type="hidden" name="redirect_on_failure" value="{
                    base_path}/ui/signin?selected_tab=magic_link" />
                <input type="hidden" name="provider"
                    value="{magic_link_provider.name}" />
                <input type="hidden" name="callback_url" value="{
                    redirect_to}" />
                {base_email_factor_form}
                {render.button("Email sign in link", id="magic-link-signin")}
            </form>
        """ if magic_link_provider else None
    )

    if email_factor_form:
        email_factor_form += render.bottom_note(
            "Don't have an account?", link='Sign up', href='signup')

    oauth_buttons = render.oauth_buttons(
        oauth_providers=oauth_providers,
        label_prefix=('Sign in with' if email_factor_form else 'Continue with'),
        challenge=challenge,
        redirect_to=redirect_to,
        redirect_to_on_signup=redirect_to_on_signup,
        collapsed=email_factor_form is not None and len(oauth_providers) >= 3,
    )

    return render.base_page(
        title=f'Sign in{f" to {app_name}" if app_name else ""}',
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error', 'email', 'selected_tab'],
        content=f'''
          {render.title('Sign in', app_name=app_name)}
          {render.error_message(error_message)}
          {oauth_buttons}
          {render.divider
           if email_factor_form and len(oauth_providers) > 0
           else ''}
          {email_factor_form or ''}
          {render.script('webauthn-authenticate') if webauthn_provider else ''}
        ''',
    )


def render_email_factor_form(
    *,
    base_email_factor_form=None,
    password_input='',
    selected_tab=None,
    single_form_fields='',
    password_form,
    webauthn_form,
    magic_link_form,
):
    if (
        password_form is None and
        webauthn_form is None and
        magic_link_form is None
    ):
        return None

    match (password_form, webauthn_form, magic_link_form):
        case (_, None, None):
            return password_form
        case (None, _, None):
            return webauthn_form
        case (None, None, _):
            return magic_link_form

    if (
        base_email_factor_form is None or
        (webauthn_form is not None and magic_link_form is not None)
    ):
        tabs = [
            ('Passkey', webauthn_form, selected_tab == 'webauthn')
            if webauthn_form else None,
            ('Password', password_form, selected_tab == 'password')
            if password_form else None,
            ('Email Link', magic_link_form, selected_tab == 'magic_link')
            if magic_link_form else None
        ]

        selected_tabs = [t[2] for t in tabs if t is not None]
        selected_index = (
            selected_tabs.index(True) if True in selected_tabs else 0)

        return (
            render.tabs_buttons(
                [t[0] for t in tabs if t is not None], selected_index) +
            render.tabs_content(
                [t[1] for t in tabs if t is not None], selected_index)
        )

    slider_content = [
        f'''
            {render.button("Sign In", id="webauthn-signin") if webauthn_form
                else render.button("Email sign in link", id="magic-link-signin")
            }
            {render.button("Sign in with password", id="show-password-form",
                    secondary=True, type="button")}
        ''',
        f'''
            {password_input}
            <div class="button-group">
                {render.button(None, id="hide-password-form",
                        secondary=True, type="button")}
                {render.button("Sign in with password", id="password-signin")}
            </div>
        '''
    ]

    return f"""
    <form id="email-factor" method="post" {
            'action="../magic-link/email"'
            if magic_link_form else ''
        } data-secondary-action="../authenticate" novalidate>
        {single_form_fields}
        {base_email_factor_form}
        {render.tabs_content(
            slider_content,
            selected_tab=(1 if selected_tab == 'password' else 0)
        )}
    </form>
    """


def render_signup_page(
    *,
    base_path: str,
    providers: frozenset,
    error_message: Optional[str] = None,
    email: Optional[str] = None,
    challenge: str,
    selected_tab: Optional[str] = None,
    # config
    redirect_to: str,
    redirect_to_on_signup: Optional[str] = None,
    app_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None,
):
    password_provider = None
    webauthn_provider = None
    magic_link_provider = None
    oauth_providers = []
    for p in providers:
        if p.name == 'builtin::local_emailpassword':
            password_provider = p
        elif p.name == 'builtin::local_webauthn':
            webauthn_provider = p
        elif p.name == 'builtin::local_magic_link':
            magic_link_provider = p
        elif p.name.startswith('builtin::oauth_'):
            oauth_providers.append(p)

    base_email_factor_form = f"""
      <input type="hidden" name="challenge" value="{challenge}" />

      <label for="email">Email</label>
      <input id="email" name="email" type="email" value="{email or ''}" />
    """

    email_factor_form = render_email_factor_form(
        selected_tab=selected_tab,
        password_form=f"""
            <form
                method="post"
                action="../register"
                novalidate
            >
                <input type="hidden" name="redirect_to" value="{
                    redirect_to_on_signup or redirect_to}" />
                <input type="hidden" name="redirect_on_failure" value="{
                    base_path}/ui/signup?selected_tab=password" />
                <input type="hidden" name="provider" value="{
                    password_provider.name}" />
                <input type="hidden" name="verify_url" value="{
                    base_path}/ui/verify" />
                {base_email_factor_form}
                <label for="password">Password</label>
                <input id="password" name="password" type="password" />
                {render.button("Sign Up", id="password-signup")}
            </form>
        """ if password_provider else None,
        webauthn_form=f"""
            <form
                id="email-factor"
                novalidate
            >
                <input type="hidden" name="redirect_to" value="{
                    redirect_to_on_signup or redirect_to}" />
                <input type="hidden" name="redirect_on_failure" value="{
                    base_path}/ui/signup?selected_tab=webauthn" />
                <input type="hidden" name="verify_url" value="{
                    base_path}/ui/verify" />
                {base_email_factor_form}
                {render.button("Sign Up", id="webauthn-signup")}
            </form>
        """ if webauthn_provider else None,
        magic_link_form=f"""
            <form
                method="post"
                action="../magic-link/register"
                novalidate
            >
                <input type="hidden" name="redirect_to" value="{
                    base_path}/ui/magic-link-sent" />
                <input type="hidden" name="redirect_on_failure" value="{
                    base_path}/ui/signup?selected_tab=magic_link" />
                <input type="hidden" name="provider" value="{
                    magic_link_provider.name}" />
                <input type="hidden" name="callback_url" value="{
                    redirect_to_on_signup or redirect_to}" />
                {base_email_factor_form}
                {render.button("Sign Up with Email Link",
                               id="magic-link-signup")}
            </form>
        """ if magic_link_provider else None
    )

    if email_factor_form:
        email_factor_form += render.bottom_note(
            'Already have an account?', link='Sign in', href='signin')

    oauth_buttons = render.oauth_buttons(
        oauth_providers=oauth_providers,
        label_prefix=('Sign up with' if email_factor_form
                      else 'Continue with'),
        challenge=challenge,
        redirect_to=redirect_to,
        redirect_to_on_signup=redirect_to_on_signup,
        collapsed=email_factor_form is not None and len(oauth_providers) >= 3
    )

    return render.base_page(
        title=f'Sign up{f" to {app_name}" if app_name else ""}',
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error', 'email', 'selected_tab'],
        content=f'''
            {render.title('Sign up', app_name=app_name)}
            {render.error_message(error_message)}
            {oauth_buttons}
            {render.divider
             if email_factor_form and len(oauth_providers) > 0
             else ''}
            {email_factor_form or ''}
            {render.script('webauthn-register') if webauthn_provider else ''}
        ''',
    )


def render_forgot_password_page(
    *,
    base_path: str,
    provider_name: str,
    challenge: str,
    error_message: Optional[str] = None,
    email: Optional[str] = None,
    email_sent: Optional[str] = None,
    # config
    app_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None,
):
    if email_sent is not None:
        content = render.success_message(
            f'Password reset email has been sent to <b>{email_sent}</b>'
        )
    else:
        content = f'''
        {render.error_message(error_message)}

        <form method="POST" action="../send-reset-email">
          <input type="hidden" name="provider" value="{provider_name}" />
          <input type="hidden" name="challenge" value="{challenge}" />
          <input type="hidden" name="redirect_on_failure" value="{
            base_path}/ui/forgot-password?challenge={challenge}" />
          <input type="hidden" name="redirect_to" value="{
            base_path}/ui/forgot-password?challenge={challenge}" />
          <input type="hidden" name="reset_url" value="{
              base_path}/ui/reset-password" />

          <label for="email">Email</label>
          <input id="email" name="email" type="email" value="{email or ''}" />

          {render.button('Send Reset Email')}
        </form>
        '''

    return render.base_page(
        title=f'Reset password{f" for {app_name}" if app_name else ""}',
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error', 'email', 'email_sent'],
        content=f'''
            {render.title('Reset password', join='for', app_name=app_name)}
            {content}
            {render.bottom_note("Back to", link="Sign In", href="signin")}
        ''',
    )


def render_reset_password_page(
    *,
    base_path: str,
    provider_name: str,
    is_valid: bool,
    redirect_to: str,
    challenge: Optional[str] = None,
    reset_token: Optional[str] = None,
    error_message: Optional[str] = None,
    # config
    app_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None,
):
    if not is_valid and challenge is None:
        content = render.error_message(
            f'''Reset token is invalid, challenge string is missing. Please
            return to the app, and attempt to log in again.''',
            False,
        )
    elif not is_valid and challenge is not None:
        content = render.error_message(
            f'''Reset token is invalid, it may have expired.
            <a href="forgot-password?challenge={challenge}">
              Try sending another reset email
            </a>''',
            False,
        )
    else:
        content = f'''
        {render.error_message(error_message)}

        <form method="POST" action="../reset-password">
          <input type="hidden" name="provider" value="{provider_name}" />
          <input type="hidden" name="reset_token" value="{reset_token}" />
          <input type="hidden" name="redirect_on_failure" value="{
            base_path}/ui/reset-password" />
          <input type="hidden" name="redirect_to" value="{redirect_to}" />

          <label for="password">New Password</label>
          <input id="password" name="password" type="password" />

          {render.button('Sign In')}
        </form>'''

    return render.base_page(
        title=f'Reset password{f" for {app_name}" if app_name else ""}',
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error'],
        content=f'''
            {render.title('Reset password', join='for', app_name=app_name)}
            {content}
        ''',
    )


def render_email_verification_page(
    *,
    is_valid: bool,
    error_messages: list[str],
    verification_token: Optional[str] = None,
    # config
    app_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None,
):
    resend_url = None
    if verification_token:
        verification_token = html.escape(verification_token)
        resend_url = (
            f"resend-verification?verification_token={verification_token}"
        )
    if not is_valid:
        messages = ''.join(
            [render.error_message(error) for error in error_messages]
        )
        content = f'''
            {messages}
            {(f'<a href="{resend_url}">Try sending another verification'
              'email</a>')
             if resend_url else ''}
            '''
    else:
        content = '''
        Email has been successfully verified. You may now
        <a href="signin">sign in</a>
        '''

    return render.base_page(
        title=f'Verify email{f" for {app_name}" if app_name else ""}',
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error'],
        content=f'''
            {render.title('Verify email', join='for', app_name=app_name)}
            {content}
        ''',
    )


def render_email_verification_expired_page(
    verification_token: str,
    # config
    app_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None,
):
    verification_token = html.escape(verification_token)
    content = render.error_message(
        f'''
        Your verification token has expired.
        <a href="resend-verification?verification_token={verification_token}">
            Click here to resend the verification email
        </a>
        ''',
        False,
    )

    return render.base_page(
        title=f'Verification expired{f" for {app_name}" if app_name else ""}',
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error'],
        content=f'''
            {render.title('Verification expired', join='for',
                          app_name=app_name)}
            {content}
        ''',
    )


def render_resend_verification_done_page(
    *,
    is_valid: bool,
    verification_token: Optional[str] = None,
    # config
    app_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None,
):
    if verification_token is None:
        content = render.error_message(
            f"""
            Missing verification token, please follow the link provided in the
            original email, or on the signin page.
            """,
            False,
        )
    else:
        verification_token = html.escape(verification_token)
        if is_valid:
            content = f'''
            Your verification email has been resent. Please check your email.
            '''
        else:
            content = f'''
            Unable to resend verification email. Please try again.
            '''

    return render.base_page(
        title=(
            f'Email verification resent{f" for {app_name}" if app_name else ""}'
        ),
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error'],
        content=f'''
            {render.title('Email verification resent', join='for',
                          app_name=app_name)}
            {content}
        ''',
    )


def render_magic_link_sent_page(
    *,
    app_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None,
):
    content = render.success_message(
        "A sign in link has been sent to your email. Please check your email."
    )
    return render.base_page(
        title=(f'Sign in link sent{f" for {app_name}" if app_name else ""}'),
        logo_url=logo_url,
        dark_logo_url=dark_logo_url,
        brand_color=brand_color,
        cleanup_search_params=['error'],
        content=f'''
            {render.title('Sign in link sent', join='for', app_name=app_name)}
            {content}
        ''',
    )

# emails


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


def render_verification_email(
    *,
    from_addr: str,
    to_addr: str,
    verify_url: str,
    app_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None,
) -> multipart.MIMEMultipart:
    msg = multipart.MIMEMultipart()
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = "Verify your email"
    alternative = multipart.MIMEMultipart('alternative')
    plain_text_msg = mime_text.MIMEText(
        f"""
        {verify_url}
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
    <a href="{verify_url}">Verify your email</a>
  </body>
</html>
        """,
        "html",
        "utf-8",
    )
    alternative.attach(html_msg)
    msg.attach(alternative)
    return msg


def render_magic_link_email(
    *,
    from_addr: str,
    to_addr: str,
    link: str,
    app_name: Optional[str] = None,
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = None,
) -> multipart.MIMEMultipart:
    msg = multipart.MIMEMultipart()
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = "Sign in link"
    alternative = multipart.MIMEMultipart('alternative')
    plain_text_msg = mime_text.MIMEText(
        f"""
        {link}
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
            <a href="{link}">Sign in</a>
          </body>
        </html>
        """,
        "html",
        "utf-8",
    )
    alternative.attach(html_msg)
    msg.attach(alternative)
    return msg
