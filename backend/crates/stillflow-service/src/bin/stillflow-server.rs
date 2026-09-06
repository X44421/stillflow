//! `stillflow-server` — the SVC-A1 service process binary (contract §4.2).

use std::path::PathBuf;

use stillflow_service::config::ProcessConfig;
use stillflow_service::process::{start_service, ProcessError};

struct Cli {
    config: Option<PathBuf>,
    port_file: Option<PathBuf>,
    bind_host: Option<String>,
    bind_port: Option<u16>,
}

fn parse_cli() -> Result<Cli, String> {
    let mut cli = Cli {
        config: None,
        port_file: None,
        bind_host: None,
        bind_port: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => cli.config = args.next().map(PathBuf::from),
            "--port-file" => cli.port_file = args.next().map(PathBuf::from),
            "--bind-host" => cli.bind_host = args.next(),
            "--bind-port" => match args.next().map(|value| value.parse::<u16>()) {
                Some(Ok(port)) => cli.bind_port = Some(port),
                _ => return Err("--bind-port requires a port number".to_owned()),
            },
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(cli)
}

async fn run(config: ProcessConfig, port_file: Option<PathBuf>) -> Result<(), String> {
    let service = start_service(config)
        .await
        .map_err(|error: ProcessError| error.to_string())?;
    let line = service.ready_line();
    println!("{line}");
    if let Some(path) = port_file {
        std::fs::write(&path, line.as_bytes())
            .map_err(|error| format!("port file write failed: {error}"))?;
    }
    let terminate = async {
        let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|error| format!("SIGTERM handler unavailable: {error}"))?;
        signal
            .recv()
            .await
            .ok_or_else(|| "SIGTERM stream closed".to_owned())
    };
    let interrupt = async {
        tokio::signal::ctrl_c()
            .await
            .map_err(|error| format!("SIGINT handler unavailable: {error}"))
    };
    tokio::select! {
        result = terminate => result?,
        result = interrupt => result?,
    }
    service
        .shutdown()
        .await
        .map_err(|error: ProcessError| error.to_string())
}

fn main() {
    let cli = match parse_cli() {
        Ok(cli) => cli,
        Err(message) => {
            eprintln!("stillflow-server: {message}");
            eprintln!(
                "usage: stillflow-server --config <path> [--bind-host H] [--bind-port P] [--port-file <path>]"
            );
            std::process::exit(2);
        }
    };
    let Some(config_path) = cli.config else {
        eprintln!("stillflow-server: --config <path> is required");
        std::process::exit(2);
    };
    let raw = match std::fs::read_to_string(&config_path) {
        Ok(raw) => raw,
        Err(error) => {
            eprintln!("stillflow-server: config read failed: {error}");
            std::process::exit(2);
        }
    };
    let mut config: ProcessConfig = match serde_json::from_str(&raw) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("stillflow-server: config parse failed: {error}");
            std::process::exit(2);
        }
    };
    if let Some(host) = cli.bind_host {
        config.service.bind_host = host;
    }
    if let Some(port) = cli.bind_port {
        config.service.bind_port = port;
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("stillflow-server: runtime build failed: {error}");
            std::process::exit(1);
        }
    };
    if let Err(message) = runtime.block_on(run(config, cli.port_file)) {
        eprintln!("stillflow-server: {message}");
        std::process::exit(1);
    }
}
