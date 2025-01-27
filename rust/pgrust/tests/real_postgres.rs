// Constants
use gel_auth::AuthType;
use gel_stream::client::{Connector, ResolvedTarget, Target, TlsParameters};
use pgrust::connection::{Credentials, PGConnectionError, RawClient};
use pgrust::errors::PgServerError;
use pgrust::handshake::ConnectionSslRequirement;
use rstest::rstest;

use captive_postgres::*;

fn address(address: &ListenAddress) -> ResolvedTarget {
    match address {
        ListenAddress::Tcp(addr) => ResolvedTarget::SocketAddr(*addr),
        #[cfg(unix)]
        ListenAddress::Unix(path) => ResolvedTarget::UnixSocketAddr(
            std::os::unix::net::SocketAddr::from_pathname(path).unwrap(),
        ),
    }
}

/// Ensure that a notice doesn't cause unexpected behavior.
#[test_log::test(tokio::test)]
async fn test_auth_noisy() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(builder) = PostgresBuilder::new().with_automatic_bin_path() else {
        return Ok(());
    };

    let builder = builder
        .debug_level(5)
        .server_option("client_min_messages", "debug5");

    let postgres_process = builder.build()?;

    let credentials = Credentials {
        username: DEFAULT_USERNAME.to_string(),
        password: DEFAULT_PASSWORD.to_string(),
        database: DEFAULT_DATABASE.to_string(),
        server_settings: Default::default(),
    };

    let ssl_requirement = ConnectionSslRequirement::Optional;

    let connector = Connector::new(Target::new_resolved_starttls(
        address(&postgres_process.socket_address),
        TlsParameters::insecure(),
    ))?;
    let raw_client = RawClient::connect(credentials, ssl_requirement, connector).await?;
    let params = raw_client.into_parts().1;
    assert_eq!(params.auth, AuthType::Trust);

    Ok(())
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_auth_real(
    #[values(AuthType::Trust, AuthType::Plain, AuthType::Md5, AuthType::ScramSha256)]
    auth: AuthType,
    #[values(Mode::Tcp, Mode::TcpSsl, Mode::Unix)] mode: Mode,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(postgres_process) = setup_postgres(auth, mode)? else {
        return Ok(());
    };

    let credentials = Credentials {
        username: DEFAULT_USERNAME.to_string(),
        password: DEFAULT_PASSWORD.to_string(),
        database: DEFAULT_DATABASE.to_string(),
        server_settings: Default::default(),
    };

    let ssl_requirement = match mode {
        Mode::TcpSsl => ConnectionSslRequirement::Required,
        _ => ConnectionSslRequirement::Optional,
    };

    let connector = Connector::new(Target::new_resolved_starttls(
        address(&postgres_process.socket_address),
        TlsParameters::insecure(),
    ))?;
    let raw_client = RawClient::connect(credentials, ssl_requirement, connector).await?;
    let params = raw_client.into_parts().1;

    assert_eq!(matches!(mode, Mode::TcpSsl), params.ssl);
    assert_eq!(auth, params.auth);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_bad_password(
    #[values(AuthType::Plain, AuthType::Md5, AuthType::ScramSha256)] auth: AuthType,
    #[values(Mode::Tcp, Mode::TcpSsl, Mode::Unix)] mode: Mode,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(postgres_process) = setup_postgres(auth, mode)? else {
        return Ok(());
    };

    let credentials = Credentials {
        username: DEFAULT_USERNAME.to_string(),
        password: "badpassword".to_string(),
        database: DEFAULT_DATABASE.to_string(),
        server_settings: Default::default(),
    };

    let ssl_requirement = match mode {
        Mode::TcpSsl => ConnectionSslRequirement::Required,
        _ => ConnectionSslRequirement::Optional,
    };

    let connector = Connector::new(Target::new_resolved_starttls(
        address(&postgres_process.socket_address),
        TlsParameters::insecure(),
    ))?;
    let raw_client = RawClient::connect(credentials, ssl_requirement, connector).await;

    assert!(
        matches!(raw_client, Err(PGConnectionError::ServerError(PgServerError { code, .. })) if &code.to_code() == b"28P01"),
        "Expected server error 28P01, got {raw_client:?}",
    );

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_bad_username(
    #[values(AuthType::Plain, AuthType::Md5, AuthType::ScramSha256)] auth: AuthType,
    #[values(Mode::Tcp, Mode::TcpSsl, Mode::Unix)] mode: Mode,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(postgres_process) = setup_postgres(auth, mode)? else {
        return Ok(());
    };

    let credentials = Credentials {
        username: "badusername".to_string(),
        password: DEFAULT_PASSWORD.to_string(),
        database: DEFAULT_DATABASE.to_string(),
        server_settings: Default::default(),
    };

    let ssl_requirement = match mode {
        Mode::TcpSsl => ConnectionSslRequirement::Required,
        _ => ConnectionSslRequirement::Optional,
    };

    let connector = Connector::new(Target::new_resolved_starttls(
        address(&postgres_process.socket_address),
        TlsParameters::insecure(),
    ))?;
    let raw_client = RawClient::connect(credentials, ssl_requirement, connector).await;

    assert!(
        matches!(raw_client, Err(PGConnectionError::ServerError(PgServerError { code, .. })) if &code.to_code() == b"28P01"),
        "Expected server error 28P01, got {:?}",
        raw_client
    );

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_bad_database(
    #[values(AuthType::Plain, AuthType::Md5, AuthType::ScramSha256)] auth: AuthType,
    #[values(Mode::Tcp, Mode::TcpSsl, Mode::Unix)] mode: Mode,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(postgres_process) = setup_postgres(auth, mode)? else {
        return Ok(());
    };

    let credentials = Credentials {
        username: DEFAULT_USERNAME.to_string(),
        password: DEFAULT_PASSWORD.to_string(),
        database: "baddatabase".to_string(),
        server_settings: Default::default(),
    };

    let ssl_requirement = match mode {
        Mode::TcpSsl => ConnectionSslRequirement::Required,
        _ => ConnectionSslRequirement::Optional,
    };

    let connector = Connector::new(Target::new_resolved_starttls(
        address(&postgres_process.socket_address),
        TlsParameters::insecure(),
    ))?;
    let raw_client = RawClient::connect(credentials, ssl_requirement, connector).await;

    assert!(
        matches!(raw_client, Err(PGConnectionError::ServerError(PgServerError { code, .. })) if &code.to_code() == b"3D000"),
        "Expected server error 3D000, got {raw_client:?}",
    );

    Ok(())
}
