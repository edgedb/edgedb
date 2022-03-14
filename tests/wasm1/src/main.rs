use edgedb_sdk::{log, web};
use edgedb_sdk::client::{Client, Error, create_client};
use once_cell::sync::Lazy;

static CLIENT: Lazy<Client> = Lazy::new(|| create_client());

fn wrap_error(f: impl FnOnce() -> Result<web::Response, Error>)
    -> web::Response
{
    match f() {
        Ok(resp) => resp,
        Err(e) => {
            log::error!("Error handling request: {:#}", e);
            web::response()
                .status(web::StatusCode::INTERNAL_SERVER_ERROR)
                .header("Content-Type", "text/plain")
                .body(format!("Internal Server Error").into())
                .expect("response is built")
        }
    }
}

#[web::handler]
fn handler(_req: web::Request) -> web::Response {
    wrap_error(|| {
        let value = CLIENT.query_required_single::<i64, _>(
            "SELECT 7*8",
            &(),
        )?;
        Ok(web::response()
            .status(web::StatusCode::OK)
            .header("Content-Type", "text/html")
            .body(format!("7 times 8 equals {value}").into())
            .expect("response is built"))
    })
}

fn main() {
}
