use std::time::Duration;

static HELP: &'static str = r#"
Example console-instrumented app

USAGE:
    app [OPTIONS]

OPTIONS:
    -h, help    prints this message
    blocks      Includes a (misbehaving) blocking task
    burn        Includes a (misbehaving) task that spins CPU with self-wakes
    coma        Includes a (misbehaving) task that forgets to register a waker
    no-norm     Excludes the behaving tree of ~30 spawned tasks
"#;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::layer::SubscriberExt;
    console_subscriber::build().with(tracing_subscriber::fmt::layer()).init();
    let mut spawn_norm = true;
    // spawn optional extras from CLI args
    // skip first which is command name
    let mut opt_handles = Vec::new();
    for opt in std::env::args().skip(1) {
        match &*opt {
            "blocks" => {
                opt_handles.push(tokio::task::Builder::new()
                                 .name("blocks")
                                 .spawn(double_sleepy(1, 10)));
            }
            "coma" => {
                opt_handles.push(tokio::task::Builder::new()
                                 .name("coma")
                                 .spawn(std::future::pending::<()>()));
            }
            "burn" => {
                opt_handles.push(tokio::task::Builder::new()
                                 .name("burn")
                                 .spawn(burn(1, 10)));
            }
            "no-norm" => {
                spawn_norm = false;
            }
            "help" | "-h" => {
                eprintln!("{}", HELP);
                return Ok(());
            }
            wat => {
                return Err(
                    format!("unknown option: {:?}, run with '-h' to see options", wat).into(),
                )
            }
        }
    }

    if spawn_norm {
        let task1 = tokio::task::Builder::new()
            .name("norm-task 1")
            .spawn(spawn_tasks(1, 10));
        let task2 = tokio::task::Builder::new()
            .name("norm-task 2")
            .spawn(spawn_tasks(10, 30));

         let result = tokio::try_join! {
             task1,
             task2,
         };
        result?;
    }

    for handle in opt_handles {
        let opt_result = tokio::try_join!(handle);
        opt_result?;
    }

    Ok(())
}

#[tracing::instrument]
async fn spawn_tasks(min: u64, max: u64) {
    loop {
        for i in min..max {
            tokio::task::Builder::new()
                .name(&format!("norm-wait({})", i))
                .spawn(wait(i));
            tokio::time::sleep(Duration::from_secs(max) - Duration::from_secs(i)).await;
        }
    }
}

#[tracing::instrument]
async fn wait(seconds: u64) {
    tokio::time::sleep(Duration::from_secs(seconds)).await;
}

#[tracing::instrument]
async fn double_sleepy(min: u64, max: u64) {
    loop {
        for i in min..max {
            // woops!
            std::thread::sleep(Duration::from_secs(i));
            tokio::time::sleep(Duration::from_secs(max - i)).await;
        }
    }
}

#[tracing::instrument]
async fn burn(min: u64, max: u64) {
    loop {
        for i in min..max {
            for _ in 0..i {
                tokio::task::yield_now().await;
            }
            tokio::time::sleep(Duration::from_secs(i - min)).await;
        }
    }
}
