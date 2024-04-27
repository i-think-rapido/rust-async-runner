pub mod prelude {
    pub use super::runner::*;
}

mod runner {
    use async_trait::async_trait;
    use std::future::Future;
    use std::sync::Arc;
    use tokio::task;

    type Producer<In> = Arc<dyn (Fn() -> ProducerResult<In>) + Send + Sync>;
    type Worker<In, Fut> = Arc<dyn (Fn(In) -> Fut) + Send + Sync>;
    type Consumer<Out> = Arc<dyn Fn(Out) -> anyhow::Result<()> + Send + Sync>;

    pub enum ProducerResult<T> {
        ContinueWith(T),
        Terminate,
    }

    pub enum RunnerType {
        Ordered,
        Unordered,
    }

    pub struct Runner<In, Out, Fut> {
        runner: Arc<dyn RunnerTrait<In, Out, Fut> + Send + Sync>,
    }

    #[async_trait]
    trait RunnerTrait<In, Out, Fut> {
        fn new<P, W, C>(capacity: usize, producer: P, worker: W, consumer: C) -> Self
        where
            P: Fn() -> ProducerResult<In> + Send + Sync + 'static,
            W: Fn(In) -> Fut + Send + Sync + 'static,
            C: Fn(Out) -> anyhow::Result<()> + Send + Sync + 'static,
            Fut: Future<Output = Out>,
            Self: Sized;
        async fn run(&self);
    }

    impl<In, Out, Fut> Runner<In, Out, Fut>
    where
        Fut: Future<Output = Out> + Send + 'static,
        In: Send + 'static,
        Out: Send + 'static,
    {
        pub fn new<P, W, C>(
            capacity: usize,
            r#type: RunnerType,
            producer: P,
            worker: W,
            consumer: C,
        ) -> Self
        where
            P: Fn() -> ProducerResult<In> + Send + Sync + 'static,
            W: Fn(In) -> Fut + Send + Sync + 'static,
            C: Fn(Out) -> anyhow::Result<()> + Send + Sync + 'static,
            Fut: Future<Output = Out>,
        {
            Self {
                runner: match r#type {
                    RunnerType::Ordered => Arc::new(ordered::OrderedRunner::new(
                        capacity, producer, worker, consumer,
                    )),
                    RunnerType::Unordered => Arc::new(unordered::UnorderedRunner::new(
                        capacity, producer, worker, consumer,
                    )),
                },
            }
        }
        pub async fn run(&self)
        {
            self.runner.run().await;
        }
    }

    mod ordered {
        use std::sync::atomic::AtomicUsize;

        use tokio::{sync::mpsc::unbounded_channel, task::yield_now};

        use super::*;

        pub struct OrderedRunner<In, Out, Fut> {
            capacity: usize,
            len: Arc<AtomicUsize>,
            producer: Producer<In>,
            worker: Worker<In, Fut>,
            consumer: Consumer<Out>,
        }

        #[async_trait]
        impl<In, Out, Fut> RunnerTrait<In, Out, Fut> for OrderedRunner<In, Out, Fut>
        where
            Fut: Future<Output = Out> + Send + 'static,
            In: Send + 'static,
            Out: Send + 'static,
        {
            fn new<P, W, C>(capacity: usize, producer: P, worker: W, consumer: C) -> Self
            where
                P: Fn() -> ProducerResult<In> + Send + Sync + 'static,
                W: Fn(In) -> Fut + Send + Sync + 'static,
                C: Fn(Out) -> anyhow::Result<()> + Send + Sync + 'static,
                Fut: Future<Output = Out>,
            {
                Self {
                    capacity,
                    len: Default::default(),
                    producer: Arc::new(producer),
                    worker: Arc::new(worker),
                    consumer: Arc::new(consumer),
                }
            }
            async fn run(&self)
            where
                Self: Send + Sync,
            {
                let (tx, mut rx) = unbounded_channel();

                let producer = self.producer.clone();
                let worker = self.worker.clone();
                let len = self.len.clone();
                let capacity = self.capacity;
                let worker_handler = task::spawn(async move {
                    while let ProducerResult::ContinueWith(value) = {
                        while len.load(std::sync::atomic::Ordering::Acquire) >= capacity {
                            yield_now().await;
                            continue;
                        }
                        len.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        producer()
                     } {
                        let worker = worker.clone();
                        let len = len.clone();
                        tx.send(task::spawn(async move {
                            let out = worker(value).await; 
                            len.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                            out
                        }))
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

    mod unordered {
        use std::sync::atomic::AtomicUsize;

        use tokio::{sync::mpsc::unbounded_channel, spawn, task::yield_now};

        use super::*;
        pub struct UnorderedRunner<In, Out, Fut> {
            capacity: usize,
            len: Arc<AtomicUsize>,
            producer: Producer<In>,
            worker: Worker<In, Fut>,
            consumer: Consumer<Out>,
        }

        #[async_trait]
        impl<In, Out, Fut> RunnerTrait<In, Out, Fut> for UnorderedRunner<In, Out, Fut>
        where
            Fut: Future<Output = Out> + Send + 'static,
            In: Send + 'static,
            Out: Send + 'static,
        {
            fn new<P, W, C>(capacity: usize, producer: P, worker: W, consumer: C) -> Self
            where
                P: Fn() -> ProducerResult<In> + Send + Sync + 'static,
                W: Fn(In) -> Fut + Send + Sync + 'static,
                C: Fn(Out) -> anyhow::Result<()> + Send + Sync + 'static,
                Fut: Future<Output = Out>,
            {
                Self {
                    capacity,
                    len: Default::default(),
                    producer: Arc::new(producer),
                    worker: Arc::new(worker),
                    consumer: Arc::new(consumer),
                }
            }
            async fn run(&self)
            {
                let (tx, mut rx) = unbounded_channel();

                let producer = self.producer.clone();
                let worker = self.worker.clone();
                let len = self.len.clone();
                let capacity = self.capacity;
                spawn(async move {
                    loop {
                        if len.load(std::sync::atomic::Ordering::Acquire) < capacity {
                            len.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            match producer() {
                                ProducerResult::ContinueWith(value) => {
                                    let worker = worker.clone();
                                    let len = len.clone();
                                    let tx = tx.clone();
                                    spawn(async move { 
                                        let out = worker(value).await;
                                        len.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                                        let _ = tx.send(out);
                                    });
                                }
                                ProducerResult::Terminate => break,
                            }
                        } else {
                            yield_now().await;
                        }
                    }
                });


                let consumer = self.consumer.clone();
                while let Some(value) = rx.recv().await {
                    let _ = consumer(value);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::prelude::*;
    use parking_lot::RwLock;
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_unordered() {
        let list = Arc::new([1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let idx = Arc::new(RwLock::new(0_usize));
        let out = Arc::new(RwLock::new([0; 9]));

        let list = list.clone();
        let idx = idx.clone();
        let output = out.clone();
        let runner = Runner::new(
            3,
            RunnerType::Unordered,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ordered() {
        let list = Arc::new([1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let idx = Arc::new(RwLock::new(0_usize));
        let out = Arc::new(RwLock::new([0; 9]));

        let list = list.clone();
        let idx = idx.clone();
        let output = out.clone();
        let runner = Runner::new(
            3,
            RunnerType::Ordered,
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
