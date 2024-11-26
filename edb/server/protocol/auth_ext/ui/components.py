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

from __future__ import annotations
from typing import Optional, TYPE_CHECKING

import html
import urllib.parse

from . import util

if TYPE_CHECKING:
    from edb.server.protocol.auth_ext import config as auth_config

known_oauth_provider_names = [
    'builtin::oauth_github',
    'builtin::oauth_google',
    'builtin::oauth_apple',
    'builtin::oauth_azure',
    'builtin::oauth_discord',
    'builtin::oauth_slack',
]


DEFAULT_BRAND_COLOR = "1f8aed"


def base_page(
    *,
    content: str,
    title: str,
    cleanup_search_params: list[str],
    logo_url: Optional[str] = None,
    dark_logo_url: Optional[str] = None,
    brand_color: Optional[str] = DEFAULT_BRAND_COLOR,
) -> bytes:
    logo = ''
    if logo_url:
        logo = '<picture class="brand-logo">'
        if dark_logo_url:
            logo += f'''<source srcset="{html.escape(dark_logo_url)}"
                media="(prefers-color-scheme: dark)" />'''
        logo += f'<img src="{html.escape(logo_url)}" /></picture>'

    cleanup_script = (
        f'''<script>
      const params = ["{'", "'.join(cleanup_search_params)}"];
      const url = new URL(location);
      if (params.some((p) => url.searchParams.has(p))) {{
        for (const p of params) {{
          url.searchParams.delete(p);
        }}
        history.replaceState(null, '', url);
      }}
    </script>'''
        if len(cleanup_search_params) > 0
        else ''
    )

    if (
        brand_color is None or
        util.hex_color_regexp.fullmatch(brand_color) is None
    ):
        brand_color = DEFAULT_BRAND_COLOR

    return f'''
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width" />
    <link rel="stylesheet" href="_static/styles.css" />
    <title>{html.escape(title)}</title>
    {cleanup_script}
    <script type="module" src="_static/interactions.js"></script>
  </head>
  <body style="{util.get_colour_vars(brand_color)}">
    {logo}
    <div id="container-wrapper" class="container-wrapper">
      <main class="container">
        {content}
      </main>
    </div>
  </body>
</html>
'''.encode()


def script(name: str) -> str:
    return f'<script type="module" src="_static/{name}.js"></script>'


def title(title: str, *, app_name: Optional[str], join: str = 'to') -> str:
    if app_name is None:
        return f'''<h1><span>{title}</span></h1>'''

    return f'''<h1><span>{title} {join}</span> {html.escape(app_name)}</h1>'''


def oauth_buttons(
    *,
    redirect_to: str,
    challenge: str,
    redirect_to_on_signup: Optional[str],
    oauth_providers: list[auth_config.OAuthProviderConfig],
    label_prefix: str,
    collapsed: bool
) -> str:
    if len(oauth_providers) == 0:
        return ''

    oauth_params = {
        'redirect_to': redirect_to,
        'challenge': challenge,
    }
    if redirect_to_on_signup:
        oauth_params['redirect_to_on_signup'] = redirect_to_on_signup

    buttons = '\n'.join(
        [
            _oauth_button(p, oauth_params, label_prefix=label_prefix)
            for p in sorted(oauth_providers, key=lambda p: p.name)
        ]
    )

    return f'''
      <div class="oauth-buttons{' collapsed' if collapsed else ''}">
        {buttons}
      </div>
    '''


def _oauth_button(
    provider: auth_config.OAuthProviderConfig,
    params: dict[str, str],
    *,
    label_prefix: str,
) -> str:
    href = '../authorize?' + urllib.parse.urlencode(
        {'provider': provider.name, **params}
    )
    if (
        provider.name.startswith('builtin::')
        and provider.name in known_oauth_provider_names
    ):
        img = f'''<img src="_static/icon_{provider.name[15:]}.svg"
            alt="{provider.display_name} Icon" />'''
    elif provider.logo_url is not None:
        img = f'''<img src="{provider.logo_url}"
            alt="{provider.display_name} Icon" />'''
    else:
        img = ''

    label = f'{label_prefix} {provider.display_name}'
    return f'''
        <a href={href} title="{label}">
          {img}
          <span>{label}</span>
        </a>
    '''


def button(
    text: Optional[str],
    *,
    id: Optional[str] = None,
    secondary: Optional[bool] = False,
    type: Optional[str] = 'submit'
) -> str:
    classes = []
    if secondary:
        classes.append('secondary')
    if text is None:
        classes.append('icon-only')

    attrs = f'type="{type}"'
    if id:
        attrs += f' id="{id}"'
    if len(classes):
        attrs += f' class="{" ".join(classes)}"'

    return f'''
      <button {attrs}>
        {f'<span>{text}</span>' if text else ''}
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


divider = '''
    <div class="divider">
      <span>or</span>
    </div>'''


def tabs_content(sections: list[str], selected_tab: int) -> str:
    content = ''

    for i, section in enumerate(sections):
        content += f'''
            <div class="slider-section{' active' if selected_tab == i else ''}">
              {section}
            </div>
        '''

    style = (
        f'style="transform: translateX({-100 * selected_tab}%)"'
        if selected_tab > 0 else ''
    )
    return f'''
        <div id="slider-container" class="slider-container" {style}>
          {content}
        </div>
    '''


_tab_underline = '''
    <svg xmlns="http://www.w3.org/2000/svg" height="2" fill="none">
      <rect height="2" width="100%" rx="1" />
    </svg>'''


def tabs_buttons(labels: list[str], selected_tab: int) -> str:
    content = ''

    for i, label in enumerate(labels):
        content += f'''
            <div class="tab{' active' if selected_tab == i else ''}">
              {label}
              {_tab_underline}
            </div>
        '''

    return f'''
        <div id="email-provider-tabs" class="tabs">
          {content}
        </div>
    '''


def hidden_input(
    *, name: str, value: str, secondary_value: Optional[str] = None
) -> str:
    return f'''<input type="hidden" name="{name}" value="{value}" {
        f'data-secondary-value="{secondary_value}"'
        if secondary_value else ''} />'''


def bottom_note(message: str, *, link: str, href: str) -> str:
    return f"""
        <div class="bottom-note">
            {message}
            <a href="{href}">{link}</a>
        </div>
        """


def error_message(message: Optional[str], escape: bool = True) -> str:
    if message is None:
        return ''

    return f'''
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
        <span>{html.escape(message) if escape else message}</span>
        </div>'''


def success_message(message: str) -> str:
    return f'''
        <div class="success-message">
        <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24"
          viewBox="0 0 24 24" fill="none">
          <path d="M22 2L11 13" stroke="currentColor" stroke-width="1.5"
            stroke-linecap="round" stroke-linejoin="round"/>
          <path d="M22 2L15 22L11 13L2 9L22 2Z" stroke="currentColor"
            stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
        <span>{message}</span>
        </div>
    '''


def base_default_email(
    *,
    content: str,
    app_name: Optional[str],
    logo_url: Optional[str],
) -> str:
    logo_html = f"""
      <!--[if mso | IE]><table align="center" border="0" cellpadding="0" cellspacing="0" class="" style="width:600px;" width="600" ><tr><td style="line-height:0px;font-size:0px;mso-line-height-rule:exactly;"><![endif]-->
      <div style="margin: 0px auto; max-width: 600px">
        <table
          align="center"
          border="0"
          cellpadding="0"
          cellspacing="0"
          role="presentation"
          style="width: 100%"
        >
          <tbody>
            <tr>
              <td
                style="
                  direction: ltr;
                  font-size: 0px;
                  padding: 20px 0;
                  padding-bottom: 0px;
                  padding-top: 20px;
                  text-align: center;
                "
              >
                <!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td class="" style="vertical-align:top;width:600px;" ><![endif]-->
                <div
                  class="mj-column-per-100 mj-outlook-group-fix"
                  style="
                    font-size: 0px;
                    text-align: left;
                    direction: ltr;
                    display: inline-block;
                    vertical-align: top;
                    width: 100%;
                  "
                >
                  <table
                    border="0"
                    cellpadding="0"
                    cellspacing="0"
                    role="presentation"
                    style="vertical-align: top"
                    width="100%"
                  >
                    <tbody>
                      <tr>
                        <td
                          align="center"
                          style="
                            font-size: 0px;
                            padding: 10px 25px;
                            padding-top: 0;
                            padding-right: 0px;
                            padding-bottom: 0px;
                            padding-left: 0px;
                            word-break: break-word;
                          "
                        >
                          <table
                            border="0"
                            cellpadding="0"
                            cellspacing="0"
                            role="presentation"
                            style="border-collapse: collapse; border-spacing: 0px"
                          >
                            <tbody>
                              <tr>
                                <td style="width: 150px">
                                  <img
                                    alt="
                                      {f'{app_name} logo' if app_name else ''}
                                    "
                                    height="150"
                                    src="{logo_url}"
                                    style="
                                      border: none;
                                      display: block;
                                      outline: none;
                                      text-decoration: none;
                                      height: 150px;
                                      width: 100%;
                                      font-size: 13px;
                                    "
                                    width="150"
                                  />
                                </td>
                              </tr>
                            </tbody>
                          </table>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
                <!--[if mso | IE]></td></tr></table><![endif]-->
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <!--[if mso | IE]></td></tr></table><table align="center" border="0" cellpadding="0" cellspacing="0" class="" style="width:600px;" width="600" ><tr><td style="line-height:0px;font-size:0px;mso-line-height-rule:exactly;"><![endif]-->
""" if logo_url else ""  # noqa: E501

    return f"""
<!doctype html>
<html
  xmlns="http://www.w3.org/1999/xhtml"
  xmlns:v="urn:schemas-microsoft-com:vml"
  xmlns:o="urn:schemas-microsoft-com:office:office"
>
<head>
  <title>
  </title>
  <!--[if !mso]><!-->
  <meta http-equiv="X-UA-Compatible" content="IE=edge">
  <!--<![endif]-->
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style type="text/css">
    #outlook a {{
      padding: 0;
    }}

    body {{
      margin: 0;
      padding: 0;
      -webkit-text-size-adjust: 100%;
      -ms-text-size-adjust: 100%;
    }}

    table,
    td {{
      border-collapse: collapse;
      mso-table-lspace: 0pt;
      mso-table-rspace: 0pt;
    }}

    img {{
      border: 0;
      height: auto;
      line-height: 100%;
      outline: none;
      text-decoration: none;
      -ms-interpolation-mode: bicubic;
    }}

    p {{
      display: block;
      margin: 13px 0;
    }}
  </style>
  <!--[if mso]>
        <noscript>
        <xml>
        <o:OfficeDocumentSettings>
          <o:AllowPNG/>
          <o:PixelsPerInch>96</o:PixelsPerInch>
        </o:OfficeDocumentSettings>
        </xml>
        </noscript>
        <![endif]-->
  <!--[if lte mso 11]>
        <style type="text/css">
          .mj-outlook-group-fix {{ width:100% !important; }}
        </style>
        <![endif]-->
  <!--[if !mso]><!-->
  <link href="https://fonts.googleapis.com/css?family=Open+Sans:300,400,500,700" rel="stylesheet" type="text/css">
  <style type="text/css">
    @import url(https://fonts.googleapis.com/css?family=Open+Sans:300,400,500,700);
  </style>
  <!--<![endif]-->
  <style type="text/css">
    @media only screen and (min-width:480px) {{
      .mj-column-per-100 {{
        width: 100% !important;
        max-width: 100%;
      }}
    }}
  </style>
  <style media="screen and (min-width:480px)">
    .moz-text-html .mj-column-per-100 {{
      width: 100% !important;
      max-width: 100%;
    }}
  </style>
  <style type="text/css">
    @media only screen and (max-width:480px) {{
      table.mj-full-width-mobile {{
        width: 100% !important;
      }}

      td.mj-full-width-mobile {{
        width: auto !important;
      }}
    }}
  </style>
</head>

  <body style="word-spacing: normal; background-color: #ffffff">
    <div style="background-color: #ffffff">
{logo_html}
      <div style="margin: 0px auto; max-width: 600px">
        <table
          align="center"
          border="0"
          cellpadding="0"
          cellspacing="0"
          role="presentation"
          style="width: 100%"
        >
          <tbody>
            <tr>
              <td
                style="
                  direction: ltr;
                  font-size: 0px;
                  padding: 20px 0;
                  padding-bottom: 20px;
                  padding-top: 20px;
                  text-align: center;
                "
              >
                <!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td class="" style="vertical-align:middle;width:600px;" ><![endif]-->
                <div
                  class="mj-column-per-100 mj-outlook-group-fix"
                  style="
                    font-size: 0px;
                    text-align: left;
                    direction: ltr;
                    display: inline-block;
                    vertical-align: middle;
                    width: 100%;
                  "
                >
                  <table
                    border="0"
                    cellpadding="0"
                    cellspacing="0"
                    role="presentation"
                    style="vertical-align: middle"
                    width="100%"
                  >
                    <tbody>
{content}
                    </tbody>
                  </table>
                </div>
                <!--[if mso | IE]></td></tr></table><![endif]-->
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <!--[if mso | IE]></td></tr></table><![endif]-->
    </div>
  </body>
</html>
"""  # noqa: E501
