use std::{iter, pin::Pin, sync::Arc, time::Duration};

use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt,
};
use object_store::{aws::AmazonS3Builder, ObjectStore, PutPayload, Result};
use tokio::task::JoinSet;

mod localstack;

static CPU_TIME: u64 = 2;
static N_FILES: usize = 2;
static OBJECT_KEY: &str = "test";

#[tokio::main]
async fn main() {
    let num_cpus = std::thread::available_parallelism().unwrap().get();

    // Start localstack container
    let localstack = localstack::localstack_container().await;
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
                execution_stream(object_store)
                    .try_collect::<Vec<Vec<_>>>()
                    .await
                    .unwrap();
            }
        });
    }

    set.join_all().await;
}

fn execution_stream(
    object_store: Arc<dyn ObjectStore>,
) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, object_store::Error>> + Send>> {
    Box::pin(
        io_stream(object_store)
            .and_then(|x| async move { cpu_work(x) })
            .and_then(|x| async move { cpu_work(x) }),
    )
}

fn io_stream(
    object_store: Arc<dyn ObjectStore>,
) -> BoxStream<'static, Result<Vec<u8>, object_store::Error>> {
    Box::pin(
        stream::iter(iter::repeat_n(object_store, N_FILES))
            .then(|object_store| async move {
                object_store
                    .get(&OBJECT_KEY.into())
                    .await
                    .unwrap()
                    .into_stream()
                    .map_ok(|x| Vec::from(x))
            })
            .flatten(),
    )
}

fn cpu_work(bytes: Vec<u8>) -> Result<Vec<u8>, object_store::Error> {
    std::thread::sleep(Duration::from_secs(CPU_TIME));
    Ok(bytes)
}
