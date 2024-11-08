#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
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


set module default;

for x in range_unpack(range(1, 1000))
union (
    # Large, varied, but deterministic dataset.
    insert L2 {vec := [x % 10, math::ln(x), x / 7 % 13]}
);


CONFIGURE CURRENT DATABASE SET ext::_conf::Config::config_name := 'ready';
CONFIGURE CURRENT DATABASE SET ext::_conf::Config::secret := 'secret';

CONFIGURE CURRENT DATABASE INSERT ext::_conf::Obj {
    name := '1',
    value := 'foo',
};
CONFIGURE CURRENT DATABASE INSERT ext::_conf::Obj {
    name := '2',
    value := 'bar',
};
CONFIGURE CURRENT DATABASE INSERT ext::_conf::SubObj {
    extra := 42,
    name := '3',
    value := 'baz',
};
CONFIGURE CURRENT DATABASE INSERT ext::_conf::SecretObj {
    name := '4',
    value := 'spam',
    secret := '123456',
};

# Lots of ext::auth config
CONFIGURE CURRENT DATABASE SET
ext::auth::AuthConfig::auth_signing_key := 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

CONFIGURE CURRENT DATABASE SET
ext::auth::AuthConfig::token_time_to_live := <duration>'24 hours';

# N.B: This CONFIGURE command was the original one, but then we
# removed that flag.  We kept it working in dumps, though, so old
# dumps still work and behave as if they had the next two statements
# instead.
#
# CONFIGURE CURRENT DATABASE SET
# ext::auth::SMTPConfig::sender := 'noreply@example.com';

CONFIGURE CURRENT DATABASE INSERT cfg::SMTPProviderConfig {
    name := "_default",
    sender := 'noreply@example.com',
};

CONFIGURE CURRENT DATABASE SET
cfg::current_email_provider_name := "_default";


CONFIGURE CURRENT DATABASE SET
ext::auth::AuthConfig::allowed_redirect_urls := {
    'https://example.com'
};

CONFIGURE CURRENT DATABASE
INSERT ext::auth::GitHubOAuthProvider {
    secret := 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
    client_id := '12f25e85-8659-41a5-87f5-9024e8b057d0',
};

CONFIGURE CURRENT DATABASE
INSERT ext::auth::GoogleOAuthProvider {
    secret := 'cccccccccccccccccccccccccccccccc',
    client_id := '798dcc1b-ab29-4aa1-9d8c-dae9f01444f2',
};

CONFIGURE CURRENT DATABASE
INSERT ext::auth::AzureOAuthProvider {
    secret := 'cccccccccccccccccccccccccccccccc',
    client_id := '1597b3fc-b67d-4d2b-b38f-acc256341dbc',
    additional_scope := 'offline_access',
};

CONFIGURE CURRENT DATABASE
INSERT ext::auth::AppleOAuthProvider {
    secret := 'cccccccccccccccccccccccccccccccc',
    client_id := 'aaf279c6-6c6e-4815-9849-d7a912d26e3b',
};

CONFIGURE CURRENT DATABASE
INSERT ext::auth::EmailPasswordProviderConfig {
    require_verification := false,
};

CONFIGURE CURRENT DATABASE INSERT ext::auth::UIConfig {
    redirect_to := 'http://example.edgedb.com'
};

INSERT L3 { x := 'satisfied customer' };
