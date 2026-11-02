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
async fn main() -> std::process::ExitCode {
    use marketforge_engine::client::management::{ClientOptions, MarketForgeManagementClient};
    let args = Args::parse();
    let client = if let Some(path) = args.token_file {
        MarketForgeManagementClient::from_token_file(&args.endpoint, path, ClientOptions::default())
    } else {
        MarketForgeManagementClient::from_environment(
            &args.endpoint,
            &args.token_env,
            ClientOptions::default(),
        )
    };
    match client {
        Ok(client) => match marketforge_engine::tui::management::run(client).await {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("marketforge-tui: {e}");
                std::process::ExitCode::FAILURE
            }
        },
        Err(e) => {
            eprintln!("marketforge-tui: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
