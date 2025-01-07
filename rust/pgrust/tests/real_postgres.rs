// Constants
use gel_auth::AuthType;
use pgrust::connection::{connect_raw_ssl, ConnectionError, Credentials, ResolvedTarget};
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

#[rstest]
#[tokio::test]
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

    let client = address(&postgres_process.socket_address).connect().await?;

    let ssl_requirement = match mode {
        Mode::TcpSsl => ConnectionSslRequirement::Required,
        _ => ConnectionSslRequirement::Optional,
    };

    let params = connect_raw_ssl(credentials, ssl_requirement, create_ssl_client()?, client)
        .await?
        .params()
        .clone();

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

    let client = address(&postgres_process.socket_address).connect().await?;

    let ssl_requirement = match mode {
        Mode::TcpSsl => ConnectionSslRequirement::Required,
        _ => ConnectionSslRequirement::Optional,
    };

    let params = connect_raw_ssl(credentials, ssl_requirement, create_ssl_client()?, client).await;
    assert!(
        matches!(params, Err(ConnectionError::ServerError(PgServerError { code, .. })) if &code.to_code() == b"28P01")
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

    let client = address(&postgres_process.socket_address).connect().await?;

    let ssl_requirement = match mode {
        Mode::TcpSsl => ConnectionSslRequirement::Required,
        _ => ConnectionSslRequirement::Optional,
    };

    let params = connect_raw_ssl(credentials, ssl_requirement, create_ssl_client()?, client).await;
    assert!(
        matches!(params, Err(ConnectionError::ServerError(PgServerError { code, .. })) if &code.to_code() == b"28P01")
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

    let client = address(&postgres_process.socket_address).connect().await?;

    let ssl_requirement = match mode {
        Mode::TcpSsl => ConnectionSslRequirement::Required,
        _ => ConnectionSslRequirement::Optional,
    };

    let params = connect_raw_ssl(credentials, ssl_requirement, create_ssl_client()?, client).await;
    assert!(
        matches!(params, Err(ConnectionError::ServerError(PgServerError { code, .. })) if &code.to_code() == b"3D000")
    );

    Ok(())
}
