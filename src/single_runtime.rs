use std::{iter, pin::Pin, sync::Arc, time::Duration};

use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt,
};
use object_store::{aws::AmazonS3Builder, ObjectStore, PutPayload, Result};
use testcontainers::{core::ExecCommand, runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::localstack::LocalStack;
use tokio::task::JoinSet;

static CPU_TIME: u64 = 2;
static N_GET: usize = 2;
static OBJECT_KEY: &str = "test";

#[tokio::main]
async fn main() {
    let num_cpus = std::thread::available_parallelism().unwrap().get();

    // Start localstack container
    let localstack = localstack_container().await;
    let localstack_host = localstack.get_host().await.unwrap();
    let localstack_port = localstack.get_host_port_ipv4(4566).await.unwrap();

    let object_store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_config("aws_access_key_id".parse().unwrap(), "user")
            .with_config("aws_secret_access_key".parse().unwrap(), "password")
            .with_config(
                "endpoint".parse().unwrap(),
                format!("http://{}:{}", localstack_host, localstack_port),
            )
            .with_config("region".parse().unwrap(), "us-east-1")
            .with_config("allow_http".parse().unwrap(), "true")
            .with_config("bucket".parse().unwrap(), "warehouse")
            .build()
            .unwrap(),
    );

    // Insert object
    object_store
        .put(
            &OBJECT_KEY.into(),
            PutPayload::from_static(&[0; 10 * 1024 * 1024]),
        )
        .await
        .unwrap();

    // Execute one stream per cpu core
    let mut set = JoinSet::new();

    for _ in 0..num_cpus {
        set.spawn({
            let object_store = object_store.clone();
            async move {
                stream(object_store)
                    .await
                    .try_collect::<Vec<Vec<_>>>()
                    .await
                    .unwrap();
            }
        });
    }

    set.join_all().await;
}

async fn stream(
    object_store: Arc<dyn ObjectStore>,
) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, object_store::Error>> + Send>> {
    Box::pin(
        stream::iter(iter::repeat_n(object_store, N_GET))
            .then(io_stream)
            .flatten()
            .and_then(cpu_work)
            .and_then(cpu_work),
    )
}

async fn io_stream(
    object_store: Arc<dyn ObjectStore>,
) -> BoxStream<'static, Result<Vec<u8>, object_store::Error>> {
    Box::pin(
        object_store
            .get(&OBJECT_KEY.into())
            .await
            .unwrap()
            .into_stream()
            .map_ok(|x| Vec::from(x)),
    )
}

async fn cpu_work(bytes: Vec<u8>) -> Result<Vec<u8>, object_store::Error> {
    std::thread::sleep(Duration::from_secs(CPU_TIME));
    Ok(bytes)
}

async fn localstack_container() -> ContainerAsync<LocalStack> {
    let localstack = LocalStack::default()
        .with_env_var("SERVICES", "s3")
        .with_env_var("AWS_ACCESS_KEY_ID", "user")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        .start()
        .await
        .unwrap();

    let command = localstack
        .exec(ExecCommand::new(vec![
            "awslocal",
            "s3api",
            "create-bucket",
            "--bucket",
            "warehouse",
        ]))
        .await
        .unwrap();

    while command.exit_code().await.unwrap().is_none() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    localstack
}
