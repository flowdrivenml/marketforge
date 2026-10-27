mod completion;
mod executor;
mod model;
mod registry;
mod scheduler;

#[path = "../rest/support.rs"]
mod rest_support;

#[path = "../../support/http_server.rs"]
mod http_server;

#[path = "executor_http.rs"]
mod executor_http;

mod coordinator;
mod coordinator_http;
mod facade;
mod ownership;
mod shutdown;
