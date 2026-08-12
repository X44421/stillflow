use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::fs::File;
use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::Range;
use std::path::Path as FilePath;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use stillflow_connector_object_store::{
    ObjectByteStream, ObjectStoreConnector, ObjectStoreCredentialResolver, S3CredentialMaterial,
};
use stillflow_connectors::SourceConnector;
use stillflow_core::{
    ConnectorError, ConnectorKind, ConnectorResult, CredentialRef, DiscoverRequest, ErrorCategory,
    InspectRequest, PreviewRequest, RequestContext, SourceConnection,
};
use tempfile::tempdir;

const BUCKET: &str = "fixture-bucket";
const ACCESS_KEY: &str = "SENTINEL_ACCESS_KEY";
const SECRET_KEY: &str = "SENTINEL_SECRET_KEY";
const SESSION_TOKEN: &str = "SENTINEL_SESSION_TOKEN";
const LAST_MODIFIED: &str = "Wed, 12 Aug 2026 00:00:00 GMT";

#[derive(Default)]
struct FixtureState {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
    uploads: Mutex<HashMap<(String, String), BTreeMap<u32, Vec<u8>>>>,
    range_gets: AtomicUsize,
    range_bytes: AtomicUsize,
    full_gets: AtomicUsize,
    aborted_uploads: AtomicUsize,
    errors: Mutex<Vec<String>>,
}

impl FixtureState {
    fn reset_reads(&self) {
        self.range_gets.store(0, Ordering::SeqCst);
        self.range_bytes.store(0, Ordering::SeqCst);
        self.full_gets.store(0, Ordering::SeqCst);
    }

    fn reads(&self) -> (usize, usize, usize) {
        (
            self.range_gets.load(Ordering::SeqCst),
            self.range_bytes.load(Ordering::SeqCst),
            self.full_gets.load(Ordering::SeqCst),
        )
    }
}

struct S3Fixture {
    address: SocketAddr,
    state: Arc<FixtureState>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl S3Fixture {
    fn start(objects: BTreeMap<String, Vec<u8>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind S3 fixture");
        listener
            .set_nonblocking(true)
            .expect("nonblocking S3 fixture");
        let address = listener.local_addr().expect("fixture address");
        let state = Arc::new(FixtureState {
            objects: Mutex::new(objects),
            ..FixtureState::default()
        });
        let stop = Arc::new(AtomicBool::new(false));
        let worker_state = Arc::clone(&state);
        let worker_stop = Arc::clone(&stop);
        let worker = thread::spawn(move || loop {
            if worker_stop.load(Ordering::SeqCst) {
                break;
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    if worker_stop.load(Ordering::SeqCst) {
                        break;
                    }
                    if let Err(error) = handle_connection(stream, &worker_state) {
                        worker_state
                            .errors
                            .lock()
                            .expect("fixture errors")
                            .push(error);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(2));
                }
                Err(error) => {
                    worker_state
                        .errors
                        .lock()
                        .expect("fixture errors")
                        .push(format!("accept failed: {error}"));
                    break;
                }
            }
        });
        Self {
            address,
            state,
            stop,
            worker: Some(worker),
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    fn assert_healthy(&self) {
        let errors = self.state.errors.lock().expect("fixture errors");
        assert!(errors.is_empty(), "S3 fixture errors: {errors:?}");
    }
}

impl Drop for S3Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

struct HttpRequest {
    method: String,
    target: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

fn read_request(stream: &mut TcpStream) -> Result<HttpRequest, String> {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|error| error.to_string())?;
    let mut received = Vec::new();
    let header_end = loop {
        if let Some(position) = received.windows(4).position(|item| item == b"\r\n\r\n") {
            break position + 4;
        }
        if received.len() > 64 * 1024 {
            return Err("request headers exceeded fixture limit".to_owned());
        }
        let mut chunk = [0_u8; 4096];
        let read = stream.read(&mut chunk).map_err(|error| error.to_string())?;
        if read == 0 {
            return Err("request ended before headers".to_owned());
        }
        received.extend_from_slice(&chunk[..read]);
    };
    let header = std::str::from_utf8(&received[..header_end])
        .map_err(|_| "request headers were not UTF-8".to_owned())?;
    let mut lines = header.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| "request line is missing".to_owned())?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| "request method is missing".to_owned())?
        .to_owned();
    let target = request_parts
        .next()
        .ok_or_else(|| "request target is missing".to_owned())?
        .to_owned();
    let mut headers = BTreeMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| "request header is malformed".to_owned())?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
    }
    let content_length = headers
        .get("content-length")
        .map_or(Ok(0_usize), |value| value.parse::<usize>())
        .map_err(|_| "request content length is invalid".to_owned())?;
    if content_length > 8 * 1024 * 1024 {
        return Err("request body exceeded fixture limit".to_owned());
    }
    let mut body = received[header_end..].to_vec();
    while body.len() < content_length {
        let remaining = content_length - body.len();
        let mut chunk = vec![0_u8; remaining.min(64 * 1024)];
        let read = stream.read(&mut chunk).map_err(|error| error.to_string())?;
        if read == 0 {
            return Err("request body ended early".to_owned());
        }
        body.extend_from_slice(&chunk[..read]);
    }
    body.truncate(content_length);
    Ok(HttpRequest {
        method,
        target,
        headers,
        body,
    })
}

fn handle_connection(mut stream: TcpStream, state: &FixtureState) -> Result<(), String> {
    let request = read_request(&mut stream)?;
    let (path, query) = request
        .target
        .split_once('?')
        .map_or((request.target.as_str(), ""), |parts| parts);
    if request.method == "GET"
        && path.trim_end_matches('/') == "/fixture-bucket"
        && query_has(query, "list-type")
    {
        let body = list_response(state)?;
        return write_response(
            &mut stream,
            "200 OK",
            vec![("Content-Type".to_owned(), "application/xml".to_owned())],
            body.len(),
            body.as_bytes(),
        );
    }
    let key = path
        .strip_prefix("/fixture-bucket/")
        .ok_or_else(|| format!("unexpected fixture path: {path}"))?;
    if request.method == "POST" && query_has(query, "uploads") {
        let upload_id = "fixture-upload".to_owned();
        state
            .uploads
            .lock()
            .map_err(|_| "uploads lock poisoned".to_owned())?
            .insert((key.to_owned(), upload_id.clone()), BTreeMap::new());
        let body = format!(
            "<InitiateMultipartUploadResult><Bucket>{BUCKET}</Bucket><Key>{key}</Key><UploadId>{upload_id}</UploadId></InitiateMultipartUploadResult>"
        );
        return write_response(
            &mut stream,
            "200 OK",
            vec![("Content-Type".to_owned(), "application/xml".to_owned())],
            body.len(),
            body.as_bytes(),
        );
    }
    if request.method == "PUT" && query_has(query, "partNumber") {
        let upload_id = query_value(query, "uploadId")
            .ok_or_else(|| "upload ID is missing".to_owned())?;
        let part_number = query_value(query, "partNumber")
            .ok_or_else(|| "part number is missing".to_owned())?
            .parse::<u32>()
            .map_err(|_| "part number is invalid".to_owned())?;
        state
            .uploads
            .lock()
            .map_err(|_| "uploads lock poisoned".to_owned())?
            .entry((key.to_owned(), upload_id.to_owned()))
            .or_default()
            .insert(part_number, request.body);
        return write_response(
            &mut stream,
            "200 OK",
            vec![("ETag".to_owned(), format!("\"part-{part_number}\""))],
            0,
            &[],
        );
    }
    if request.method == "POST" && query_has(query, "uploadId") {
        let upload_id = query_value(query, "uploadId")
            .ok_or_else(|| "upload ID is missing".to_owned())?;
        let parts = state
            .uploads
            .lock()
            .map_err(|_| "uploads lock poisoned".to_owned())?
            .remove(&(key.to_owned(), upload_id.to_owned()))
            .ok_or_else(|| "multipart upload was not initiated".to_owned())?;
        let mut object = Vec::new();
        for part in parts.into_values() {
            object.extend_from_slice(&part);
        }
        state
            .objects
            .lock()
            .map_err(|_| "objects lock poisoned".to_owned())?
            .insert(key.to_owned(), object);
        let body = format!(
            "<CompleteMultipartUploadResult><Bucket>{BUCKET}</Bucket><Key>{key}</Key><ETag>\"complete-etag\"</ETag></CompleteMultipartUploadResult>"
        );
        return write_response(
            &mut stream,
            "200 OK",
            vec![
                ("Content-Type".to_owned(), "application/xml".to_owned()),
                ("ETag".to_owned(), "\"complete-etag\"".to_owned()),
            ],
            body.len(),
            body.as_bytes(),
        );
    }
    if request.method == "DELETE" && query_has(query, "uploadId") {
        let upload_id = query_value(query, "uploadId")
            .ok_or_else(|| "upload ID is missing".to_owned())?;
        state
            .uploads
            .lock()
            .map_err(|_| "uploads lock poisoned".to_owned())?
            .remove(&(key.to_owned(), upload_id.to_owned()));
        state.aborted_uploads.fetch_add(1, Ordering::SeqCst);
        return write_response(&mut stream, "204 No Content", Vec::new(), 0, &[]);
    }
    if request.method == "PUT" && query.is_empty() {
        let length = request.body.len();
        state
            .objects
            .lock()
            .map_err(|_| "objects lock poisoned".to_owned())?
            .insert(key.to_owned(), request.body);
        return write_response(
            &mut stream,
            "200 OK",
            object_headers(length),
            0,
            &[],
        );
    }
    let object = state
        .objects
        .lock()
        .map_err(|_| "objects lock poisoned".to_owned())?
        .get(key)
        .cloned();
    let Some(object) = object else {
        return not_found(&mut stream, key);
    };
    match request.method.as_str() {
        "HEAD" => write_response(
            &mut stream,
            "200 OK",
            object_headers(object.len()),
            object.len(),
            &[],
        ),
        "GET" => {
            if let Some(value) = request.headers.get("range") {
                let range = parse_range(value, object.len())?;
                let body = object
                    .get(range.clone())
                    .ok_or_else(|| "requested range is outside the object".to_owned())?;
                state.range_gets.fetch_add(1, Ordering::SeqCst);
                state.range_bytes.fetch_add(body.len(), Ordering::SeqCst);
                let mut headers = object_headers(body.len());
                headers.push((
                    "Content-Range".to_owned(),
                    format!("bytes {}-{}/{}", range.start, range.end - 1, object.len()),
                ));
                write_response(&mut stream, "206 Partial Content", headers, body.len(), body)
            } else {
                state.full_gets.fetch_add(1, Ordering::SeqCst);
                write_response(
                    &mut stream,
                    "200 OK",
                    object_headers(object.len()),
                    object.len(),
                    &object,
                )
            }
        }
        _ => write_response(&mut stream, "405 Method Not Allowed", Vec::new(), 0, &[]),
    }
}

fn list_response(state: &FixtureState) -> Result<String, String> {
    let objects = state
        .objects
        .lock()
        .map_err(|_| "objects lock poisoned".to_owned())?;
    let mut body = format!(
        "<ListBucketResult><Name>{BUCKET}</Name><IsTruncated>false</IsTruncated><KeyCount>{}</KeyCount>",
        objects.len()
    );
    for (key, value) in objects.iter() {
        write!(
            body,
            "<Contents><Key>{key}</Key><LastModified>2026-08-12T00:00:00Z</LastModified><ETag>\"etag-{}\"</ETag><Size>{}</Size></Contents>",
            value.len(),
            value.len()
        )
        .map_err(|_| "list response formatting failed".to_owned())?;
    }
    body.push_str("</ListBucketResult>");
    Ok(body)
}

fn object_headers(length: usize) -> Vec<(String, String)> {
    vec![
        ("ETag".to_owned(), format!("\"etag-{length}\"")),
        ("Last-Modified".to_owned(), LAST_MODIFIED.to_owned()),
        ("x-amz-version-id".to_owned(), "fixture-version".to_owned()),
    ]
}

fn not_found(stream: &mut TcpStream, key: &str) -> Result<(), String> {
    let body = format!(
        "<Error><Code>NoSuchKey</Code><Message>missing fixture object</Message><Key>{key}</Key></Error>"
    );
    write_response(
        stream,
        "404 Not Found",
        vec![("Content-Type".to_owned(), "application/xml".to_owned())],
        body.len(),
        body.as_bytes(),
    )
}

fn write_response(
    stream: &mut TcpStream,
    status: &str,
    headers: Vec<(String, String)>,
    content_length: usize,
    body: &[u8],
) -> Result<(), String> {
    let mut response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {content_length}\r\nConnection: close\r\n"
    );
    for (name, value) in headers {
        write!(response, "{name}: {value}\r\n")
            .map_err(|_| "response formatting failed".to_owned())?;
    }
    response.push_str("\r\n");
    stream
        .write_all(response.as_bytes())
        .and_then(|()| stream.write_all(body))
        .and_then(|()| stream.flush())
        .map_err(|error| error.to_string())
}

fn parse_range(value: &str, length: usize) -> Result<Range<usize>, String> {
    let value = value
        .strip_prefix("bytes=")
        .ok_or_else(|| "range unit is invalid".to_owned())?;
    let (start, end) = value
        .split_once('-')
        .ok_or_else(|| "range is malformed".to_owned())?;
    let start = start
        .parse::<usize>()
        .map_err(|_| "range start is invalid".to_owned())?;
    let end = end
        .parse::<usize>()
        .map_err(|_| "range end is invalid".to_owned())?
        .checked_add(1)
        .ok_or_else(|| "range end overflowed".to_owned())?;
    if start >= end || end > length {
        return Err("range is outside the object".to_owned());
    }
    Ok(start..end)
}

fn query_has(query: &str, key: &str) -> bool {
    query.split('&').any(|item| {
        item.split_once('=')
            .map_or(item, |(name, _)| name)
            .eq(key)
    })
}

fn query_value<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query.split('&').find_map(|item| {
        let (name, value) = item.split_once('=').unwrap_or((item, ""));
        (name == key).then_some(value)
    })
}

#[derive(Debug)]
struct FixtureResolver {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ObjectStoreCredentialResolver for FixtureResolver {
    async fn resolve_s3(
        &self,
        credential_ref: &CredentialRef,
    ) -> ConnectorResult<S3CredentialMaterial> {
        assert_eq!(credential_ref.as_str(), "cred://tests/object-store");
        self.calls.fetch_add(1, Ordering::SeqCst);
        S3CredentialMaterial::new(
            ACCESS_KEY,
            SECRET_KEY,
            Some(SESSION_TOKEN.to_owned()),
        )
    }
}

#[derive(Debug)]
struct PanicResolver;

#[async_trait]
impl ObjectStoreCredentialResolver for PanicResolver {
    async fn resolve_s3(
        &self,
        _credential_ref: &CredentialRef,
    ) -> ConnectorResult<S3CredentialMaterial> {
        panic!("anonymous storage must not resolve credentials")
    }
}

fn connection(fixture: &S3Fixture, anonymous: bool) -> SourceConnection {
    SourceConnection::try_new(
        ConnectorKind::ObjectStore,
        "S3 fixture",
        serde_json::json!({
            "provider": "s3",
            "bucket": BUCKET,
            "region": "us-east-1",
            "endpoint": fixture.endpoint(),
            "pathStyle": true,
            "anonymous": anonymous,
            "allowHttp": true,
            "maxPreviewSourceBytes": 65536
        }),
        CredentialRef::new("cred://tests/object-store").expect("credential reference"),
    )
    .expect("S3 fixture connection")
}

fn csv_fixture() -> Vec<u8> {
    let mut output = String::from("id,label\n");
    let payload = "x".repeat(128);
    for index in 0..10_000 {
        writeln!(output, "{index},{payload}-{index:05}").expect("CSV fixture row");
    }
    output.into_bytes()
}

fn parquet_fixture(root: &FilePath) -> Vec<u8> {
    let path = root.join("rows.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let row_count = 4_096_i64;
    let ids = (0..row_count).collect::<Vec<_>>();
    let labels = (0..row_count)
        .map(|index| format!("label-{index:05}-{}", "y".repeat(96)))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(labels)),
        ],
    )
    .expect("Parquet batch");
    let properties = WriterProperties::builder()
        .set_max_row_group_size(64)
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).expect("Parquet fixture file"),
        schema,
        Some(properties),
    )
    .expect("Parquet writer");
    writer.write(&batch).expect("write Parquet fixture");
    writer.close().expect("close Parquet fixture");
    std::fs::read(path).expect("read Parquet fixture")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_s3_adapter_is_bounded_streaming_secret_safe_and_abortable() {
    let directory = tempdir().expect("Parquet fixture directory");
    let csv = csv_fixture();
    let parquet = parquet_fixture(directory.path());
    let fixture = S3Fixture::start(BTreeMap::from([
        ("large.csv".to_owned(), csv.clone()),
        ("rows.parquet".to_owned(), parquet.clone()),
    ]));
    let calls = Arc::new(AtomicUsize::new(0));
    let connector = ObjectStoreConnector::new(Arc::new(FixtureResolver {
        calls: Arc::clone(&calls),
    }));
    let connection = connection(&fixture, false);
    let context = RequestContext::new();
    let access = connector
        .open_access(&connection, &context)
        .await
        .expect("open real S3 adapter");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let listed = access.list("", &context).await.expect("list S3 objects");
    assert_eq!(
        listed
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        ["large.csv", "rows.parquet"]
    );
    assert_eq!(
        access
            .head("large.csv", &context)
            .await
            .expect("head S3 object")
            .size,
        csv.len() as u64
    );
    assert_eq!(
        access
            .get_range("large.csv", 0..8, &context)
            .await
            .expect("S3 range"),
        Bytes::copy_from_slice(&csv[..8])
    );
    let streamed = access
        .stream("large.csv", &context)
        .await
        .expect("open S3 stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("read S3 stream")
        .concat();
    assert_eq!(streamed, csv);

    let upload: ObjectByteStream = Box::pin(futures::stream::iter([
        Ok(Bytes::from_static(b"hello ")),
        Ok(Bytes::from_static(b"object storage")),
    ]));
    let uploaded = access
        .upload("nested/output.txt", upload, &context)
        .await
        .expect("multipart upload");
    assert_eq!(uploaded.size, 20);
    let uploaded_bytes = access
        .stream("nested/output.txt", &context)
        .await
        .expect("open uploaded object")
        .try_collect::<Vec<_>>()
        .await
        .expect("read uploaded object")
        .concat();
    assert_eq!(uploaded_bytes, b"hello object storage");

    let failed_upload: ObjectByteStream = Box::pin(futures::stream::iter([
        Ok(Bytes::from_static(b"partial")),
        Err(ConnectorError::internal("fixture upload failure")),
    ]));
    let error = access
        .upload("aborted.txt", failed_upload, &context)
        .await
        .expect_err("source failure aborts multipart upload");
    assert_eq!(error.category(), ErrorCategory::Internal);
    assert_eq!(fixture.state.aborted_uploads.load(Ordering::SeqCst), 1);

    let missing = access
        .head("missing.csv", &context)
        .await
        .expect_err("missing object");
    assert_eq!(missing.category(), ErrorCategory::NotFound);
    let public_error = format!(
        "{missing:?} {missing} {}",
        serde_json::to_string(&missing.sanitized_summary()).expect("error summary")
    );
    for secret in [ACCESS_KEY, SECRET_KEY, SESSION_TOKEN] {
        assert!(!public_error.contains(secret));
    }

    let capabilities = connector.capabilities();
    assert!(capabilities.streaming);
    assert!(capabilities.range_read);
    assert!(capabilities.column_projection);
    assert!(!capabilities.predicate_pushdown);

    let anonymous = ObjectStoreConnector::new(Arc::new(PanicResolver));
    anonymous
        .open_access(&connection(&fixture, true), &RequestContext::new())
        .await
        .expect("anonymous S3 adapter");

    let assets = connector
        .discover(
            &connection,
            DiscoverRequest {
                context: RequestContext::new(),
                parent_path: None,
            },
        )
        .await
        .expect("discover S3 tabular objects");
    let csv_asset = assets
        .iter()
        .find(|asset| asset.name == "large.csv")
        .expect("CSV asset")
        .clone();
    fixture.state.reset_reads();
    let csv_preview = connector
        .preview(
            &connection,
            PreviewRequest::new(csv_asset, 10, 1024 * 1024),
        )
        .await
        .expect("bounded CSV preview");
    assert!(csv_preview.rows_returned > 0);
    assert!(csv_preview.rows_truncated);
    assert!(csv_preview.bytes_truncated);
    let (range_gets, range_bytes, full_gets) = fixture.state.reads();
    assert!(range_gets > 0);
    assert!(range_bytes <= 65_536);
    assert_eq!(full_gets, 0);

    let parquet_asset = assets
        .iter()
        .find(|asset| asset.name == "rows.parquet")
        .expect("Parquet asset")
        .clone();
    fixture.state.reset_reads();
    let metadata = connector
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::new(),
                asset: parquet_asset.clone(),
            },
        )
        .await
        .expect("range-native Parquet inspection");
    assert_eq!(metadata.row_count, Some(4_096));
    let (range_gets, range_bytes, full_gets) = fixture.state.reads();
    assert!(range_gets >= 2);
    assert!(range_bytes < parquet.len());
    assert_eq!(full_gets, 0);

    fixture.state.reset_reads();
    let parquet_preview = connector
        .preview(
            &connection,
            PreviewRequest::new(parquet_asset, 10, 1024 * 1024),
        )
        .await
        .expect("range-native Parquet preview");
    assert_eq!(parquet_preview.rows_returned, 10);
    assert!(parquet_preview.rows_truncated);
    let (range_gets, range_bytes, full_gets) = fixture.state.reads();
    assert!(range_gets >= 3);
    assert!(range_bytes < parquet.len());
    assert_eq!(full_gets, 0);
    fixture.assert_healthy();
}
