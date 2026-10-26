use marketforge_engine::live::infrastructure::websocket::{
    StreamBinding, StreamInput, StreamSource, WebSocketManager, WebSocketManagerError,
    WebSocketManagerRuntime, WebSocketProfileRuntime,
};

#[test]
fn websocket_manager_public_api_is_accessible() {
    fn accept_manager(_: &WebSocketManager) {}

    fn accept_binding(_: StreamBinding) {}

    fn accept_input(_: StreamInput) {}

    fn accept_source(_: StreamSource) {}

    fn accept_error(_: WebSocketManagerError) {}

    fn accept_runtime(_: WebSocketManagerRuntime) {}

    fn accept_profile(_: WebSocketProfileRuntime) {}

    let _ = (
        accept_manager,
        accept_binding,
        accept_input,
        accept_source,
        accept_error,
        accept_runtime,
        accept_profile,
    );
}
