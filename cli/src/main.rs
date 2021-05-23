use distill_cli::{shell::Shell, *};

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let local = async_executor::LocalExecutor::new();
    async_io::block_on(local.run(async_main(&local)))
}

async fn async_main(local: &async_executor::LocalExecutor<'_>) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = create_context(local).await?;

    let mut shell = Shell::new(ctx);

    shell.register_command("pack", CmdPack);
    shell.register_command("show_all", CmdShowAll);
    shell.register_command("get", CmdGet);
    shell.register_command("build", CmdBuild);
    shell.register_command("path_for_asset", CmdPathForAsset);
    shell.register_command("assets_for_path", CmdAssetsForPath);

    shell.run_repl().await
}
