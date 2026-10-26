use std::{
    collections::VecDeque,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

#[derive(Clone)]
pub(crate) struct MockResponse {
    status: u16,
    body: &'static str,
    retry_after: Option<&'static str>,
}

impl MockResponse {
    pub(crate) fn ok(body: &'static str) -> Self {
        Self {
            status: 200,
            body,
            retry_after: None,
        }
    }

    pub(crate) fn error(status: u16, body: &'static str) -> Self {
        Self {
            status,
            body,
            retry_after: None,
        }
    }

    pub(crate) fn throttled(retry_after: &'static str) -> Self {
        Self {
            status: 429,
            body: r#"{"code":-1003,"msg":"Too many requests"}"#,
            retry_after: Some(retry_after),
        }
    }
}

// ============================================================
// REQUEST OBSERVATION
// ============================================================

#[derive(Debug, Clone)]
pub(crate) struct ObservedRequest {
    pub(crate) received_at: Instant,
    pub(crate) request_line: String,
}

// ============================================================
// LOCAL MOCK EXCHANGE
// ============================================================

pub(crate) struct MockExchange {
    address: String,

    running: Arc<AtomicBool>,

    requests: Arc<Mutex<Vec<ObservedRequest>>>,

    worker: Option<thread::JoinHandle<()>>,
}

impl MockExchange {
    pub(crate) fn start(responses: Vec<MockResponse>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();

        listener.set_nonblocking(true).unwrap();

        let address = listener.local_addr().unwrap();

        let running = Arc::new(AtomicBool::new(true));

        let requests = Arc::new(Mutex::new(Vec::<ObservedRequest>::new()));

        let script = Arc::new(Mutex::new(VecDeque::from(responses)));

        let worker_running = running.clone();
        let worker_requests = requests.clone();

        let worker = thread::spawn(move || {
            while worker_running.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let request_line = read_request_line(&mut stream);

                        worker_requests.lock().unwrap().push(ObservedRequest {
                            received_at: Instant::now(),
                            request_line,
                        });

                        let response = script.lock().unwrap().pop_front().unwrap_or_else(|| {
                            MockResponse::error(500, r#"{"error":"unexpected request"}"#)
                        });

                        write_response(&mut stream, &response);
                    }

                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }

                    Err(_) => break,
                }
            }
        });

        Self {
            address: format!("http://{address}"),
            running,
            requests,
            worker: Some(worker),
        }
    }

    pub(crate) fn base_url(&self) -> &str {
        &self.address
    }

    pub(crate) fn requests(&self) -> Vec<ObservedRequest> {
        self.requests.lock().unwrap().clone()
    }

    pub(crate) fn request_count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

impl Drop for MockExchange {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);

        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
    }
}

// ============================================================
// HTTP HELPERS
// ============================================================

fn read_request_line(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();

    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];

    while buffer.len() < 16 * 1024 {
        match stream.read(&mut chunk) {
            Ok(0) => break,

            Ok(count) => {
                buffer.extend_from_slice(&chunk[..count]);

                if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }

            Err(_) => break,
        }
    }

    let text = String::from_utf8_lossy(&buffer);

    text.lines().next().unwrap_or_default().to_string()
}

fn write_response(stream: &mut TcpStream, response: &MockResponse) {
    let reason = match response.status {
        200 => "OK",
        400 => "Bad Request",
        429 => "Too Many Requests",
        503 => "Service Unavailable",
        _ => "Error",
    };

    let retry_after = response
        .retry_after
        .map(|value| format!("Retry-After: {value}\r\n"))
        .unwrap_or_default();

    let payload = format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         {}\r\n\
         {}",
        response.status,
        reason,
        response.body.len(),
        retry_after,
        response.body,
    );

    let _ = stream.write_all(payload.as_bytes());
    let _ = stream.flush();
}
