use hyper::header::CONTENT_TYPE;
use tower::ServiceBuilder;
use tower_http::{set_header::SetResponseHeaderLayer, ServiceBuilderExt};

// pub fn build_tower() -> impl tower::Service<hyper::http::Request<hyper::body::Incoming>> {
//     ServiceBuilder::new()
//         // Compress response bodies
//         .compression()
//         // Deconpress request bodies
//         .decompression()
//         .service_fn(f)
// }
