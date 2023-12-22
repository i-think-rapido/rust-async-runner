pub mod prelude {
    pub use super::runner::*;
}

mod runner {
    use std::future::Future;
    use std::sync::Arc;
    use tokio::sync::mpsc::channel;
    use tokio::task;

    type Producer<In> = Arc<dyn (Fn() -> ProducerResult<In>) + Send + Sync>;
    type Worker<In, Fut> = Arc<dyn (Fn(In) -> Fut) + Send + Sync>;
    type Consumer<Out> = Arc<dyn Fn(Out) -> anyhow::Result<()> + Send + Sync>;

    pub enum ProducerResult<T> {
        ContinueWith(T),
        Terminate,
    }

    pub struct AsyncRunner<In, Out, Fut> {
        capacity: usize,
        producer: Producer<In>,
        worker: Worker<In, Fut>,
        consumer: Consumer<Out>,
    }

    impl<In, Out, Fut> AsyncRunner<In, Out, Fut>
    where
        Fut: Future<Output = Out> + Send + 'static,
        In: Send + 'static,
        Out: Send + 'static,
    {
        pub fn new<P, W, C>(capacity: usize, producer: P, worker: W, consumer: C) -> Self
        where
            P: Fn() -> ProducerResult<In> + Send + Sync + 'static,
            W: Fn(In) -> Fut + Send + Sync + 'static,
            C: Fn(Out) -> anyhow::Result<()> + Send + Sync + 'static,
            Fut: Future<Output = Out>,
        {
            Self {
                capacity,
                producer: Arc::new(producer),
                worker: Arc::new(worker),
                consumer: Arc::new(consumer),
            }
        }

        pub async fn run(&self) {
            let (tx, mut rx) = channel(self.capacity);

            let producer = self.producer.clone();
            let worker = self.worker.clone();
            let worker_handler = task::spawn(async move {
                while let ProducerResult::ContinueWith(value) = producer() {
                    let worker = worker.clone();
                    tx.send(task::spawn(async move { worker(value).await }))
                        .await
                        .expect("Can't send new spawned worker");
                }
            });

            let consumer = self.consumer.clone();
            let consumer_handler = task::spawn(async move {
                while let Some(handler) = rx.recv().await {
                    let _ = consumer(handler.await.unwrap());
                }
            });

            let _ = worker_handler.await;
            let _ = consumer_handler.await;
        }
    }
}

#[cfg(test)]
mod test {
    use super::prelude::*;
    use parking_lot::RwLock;
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test() {
        let list = Arc::new([1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let idx = Arc::new(RwLock::new(0_usize));
        let out = Arc::new(RwLock::new([0; 9]));

        let list = list.clone();
        let idx = idx.clone();
        let output = out.clone();
        let runner = AsyncRunner::new(
            3,
            /* producer */
            move || {
                let out = if *idx.read() < list.len() {
                    ProducerResult::ContinueWith((*idx.read(), list[*idx.read()]))
                } else {
                    ProducerResult::Terminate
                };
                *idx.write() += 1;
                out
            },
            /* worker */ |(idx, item)| async move { (idx, item * 2) },
            /* consumer */
            move |(idx, item)| {
                output.write()[idx] = item;
                Ok(())
            },
        );

        runner.run().await;

        assert_eq!(*out.read(), [2, 4, 6, 8, 10, 12, 14, 16, 18]);
    }
}
