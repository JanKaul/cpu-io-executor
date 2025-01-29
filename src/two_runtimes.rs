use std::{iter, pin::Pin, sync::Arc, time::Duration};

use dedicated_executor::DedicatedExecutor;
use futures::{
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt,
};
use object_store::{aws::AmazonS3Builder, ObjectStore, PutPayload, Result};

mod dedicated_executor;
mod localstack;

static CPU_TIME: u64 = 2;
static N_FILES: usize = 2;
static OBJECT_KEY: &str = "test";
static N_IO_THREADS: usize = 2;

#[tokio::main(worker_threads = 2)]
async fn main() {
    let num_threads = std::thread::available_parallelism().unwrap().get();

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

    let dedicated_executor = DedicatedExecutor::builder()
        .with_worker_threads(num_threads - N_IO_THREADS)
        .build();

    let io_object_store = dedicated_executor.wrap_object_store_for_io(object_store);

    let mut handles = Vec::new();

    // Leave two cores unoccupied
    for _ in 0..(num_threads - N_IO_THREADS) {
        let handle = dedicated_executor
            .spawn({
                let io_object_store = io_object_store.clone();
                async move { execution_stream(io_object_store) }
            })
            .await
            .unwrap();
        handles.push(handle);
    }

    for handle in handles {
        dedicated_executor
            .run_cpu_stream(handle, |err| object_store::Error::Generic {
                store: "s3",
                source: Box::new(err),
            })
            .try_collect::<Vec<Vec<_>>>()
            .await
            .unwrap();
    }

    dedicated_executor.join().await;
}

fn execution_stream(
    object_store: Arc<dyn ObjectStore>,
) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, object_store::Error>> + Send>> {
    Box::pin(io_stream(object_store).map_ok(cpu_work).map_ok(cpu_work))
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

fn cpu_work(bytes: Vec<u8>) -> Vec<u8> {
    std::thread::sleep(Duration::from_secs(CPU_TIME));
    bytes
}
