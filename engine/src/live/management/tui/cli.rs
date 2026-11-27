use clap::Parser;
#[derive(Parser)]
#[command(about = "Standalone MarketForge administration; q exits only this UI")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    endpoint: String,
    #[arg(long)]
    token_file: Option<std::path::PathBuf>,
    #[arg(long, default_value = "MARKETFORGE_MANAGEMENT_TOKEN")]
    token_env: String,
}
#[tokio::main]
pub async fn run() -> std::process::ExitCode {
    use crate::live::management::client::{ClientOptions, MarketForgeManagementClient};
    let args = Args::parse();
    let client = MarketForgeManagementClient::resolve_credentials(
        &args.endpoint,
        args.token_file.as_deref(),
        &args.token_env,
        ClientOptions::default(),
    );
    match client {
        Ok(client) => match super::run(client).await {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("marketforge-live-tui: {e}");
                std::process::ExitCode::FAILURE
            }
        },
        Err(e) => {
            eprintln!("marketforge-live-tui: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
