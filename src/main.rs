use bytes::{Buf, Bytes};
use std::{future::Future, sync::Arc};

// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct Message<'a> {
    key: &'a [u8],
    payload: &'a [u8],
}
impl<'a> Message<'a> {
    pub fn detach(self) -> MessageDetached {
        MessageDetached {
            key: self.key.to_vec().into(),
            payload: self.payload.to_vec().into(),
        }
    }
}

#[derive(Debug)]
pub struct MessageDetached {
    key: Bytes,
    payload: Bytes,
}
impl Clone for MessageDetached {
    fn clone(&self) -> Self {
        Self {
            key: Bytes::clone(&self.key),     // be explicit
            payload: Bytes::clone(&self.key), // be explicit
        }
    }
}

// -----------------------------------------------------------------------------

pub struct ProduceRequest<'a> {
    pub topic: &'a str,
    pub partition: Option<i32>,

    pub key: Option<&'a Bytes>,
    pub payload: Option<&'a dyn Buf>,

    pub timestamp: Option<i64>,
    // pub headers: Option<OwnedHeaders>,
}

pub trait Partitioner {
    fn partition(&self, nb_partitions: usize, req: &ProduceRequest<'_>) -> i32;
}
impl<F> Partitioner for F
where
    F: Fn(usize, &ProduceRequest<'_>) -> i32,
{
    fn partition(&self, nb_partitions: usize, req: &ProduceRequest<'_>) -> i32 {
        (self)(nb_partitions, req)
    }
}

/// A Producer must be cheaply cloneable.
pub trait Producer: Clone {
    type Downcast: Clone;
    type ProduceResultFut: Future<Output = Result<(), String>>;

    fn send(&self, req: ProduceRequest<'_>) -> Self::ProduceResultFut;
    fn flush(&self) -> Self::ProduceResultFut;

    fn downcast(&self) -> Self::Downcast;
}

// -----------------------------------------------------------------------------

// Producer constraints?
// - max buffered time - global & per partition
// - max buffered bytes - global & per partition
// - max buffered messages - global & per partition
pub struct ProducerConfig {
    partitioner: Arc<dyn Partitioner>,
}
impl Clone for ProducerConfig {
    fn clone(&self) -> Self {
        Self {
            partitioner: Arc::clone(&self.partitioner), // be explicit
        }
    }
}

#[derive(Debug)]
pub struct TopicConfig {
    name: String,
    nb_partitions: usize,
}
impl TopicConfig {
    pub fn new<S: Into<String>>(name: S, nb_partitions: usize) -> Self {
        Self {
            name: name.into(),
            nb_partitions,
        }
    }
}

// -----------------------------------------------------------------------------

pub mod mem {
    use super::{ProduceRequest, Producer as ProducerTrait, ProducerConfig, TopicConfig};
    use bytes::{Buf, Bytes};
    use parking_lot::{Mutex, RwLock};
    use std::{
        collections::HashMap,
        future::Future,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    };

    /// Cheaply cloneable.
    pub struct Cluster {
        name: String,
        inner: Arc<ClusterInner>,
    }
    struct ClusterInner {
        topics: RwLock<HashMap<String, Topic>>,
    }
    impl Cluster {
        pub fn builder<S: Into<String>>(name: S) -> ClusterBuilder {
            ClusterBuilder::new(name)
        }

        pub fn new_producer(&self, conf: ProducerConfig) -> Producer {
            Producer {
                cluster: self.clone(),
                conf,
            }
        }
    }
    impl Clone for Cluster {
        fn clone(&self) -> Self {
            Self {
                name: self.name.clone(),
                inner: Arc::clone(&self.inner), // be explicit
            }
        }
    }
    impl std::fmt::Debug for Cluster {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct(&self.name).finish()
        }
    }

    struct Topic {
        partitions: Vec<RwLock<Vec<Bytes>>>,
    }
    impl Topic {
        pub fn new(nb_partitions: usize) -> Self {
            let partitions = std::iter::repeat_with(|| RwLock::new(Vec::new()))
                .take(nb_partitions)
                .collect();
            Self { partitions }
        }
    }

    pub struct ClusterBuilder {
        name: String,
        topics: HashMap<String, TopicConfig>,
    }
    impl ClusterBuilder {
        fn new<S: Into<String>>(name: S) -> Self {
            Self {
                name: name.into(),
                topics: Default::default(),
            }
        }

        pub fn with_topic(mut self, conf: TopicConfig) -> Self {
            self.topics.insert(conf.name.clone(), conf);
            self
        }

        pub fn build(self) -> Cluster {
            let topics: HashMap<_, _> = self
                .topics
                .into_iter()
                .map(|(name, conf)| (name, Topic::new(conf.nb_partitions)))
                .collect();
            let topics = RwLock::new(topics);

            let inner = Arc::new(ClusterInner { topics });
            Cluster {
                name: self.name,
                inner,
            }
        }
    }

    // -----------------------------------------------------------------------------

    pub struct MemProduceResultFut;
    impl Future for MemProduceResultFut {
        type Output = Result<(), String>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Clone)]
    pub struct Producer {
        cluster: Cluster,
        conf: ProducerConfig,
    }
    impl Producer {
        pub fn new(cluster: Cluster, conf: ProducerConfig) -> Self {
            Self { cluster, conf }
        }
    }
    impl ProducerTrait for Producer {
        type Downcast = Producer;
        type ProduceResultFut = MemProduceResultFut;

        fn send(&self, req: crate::ProduceRequest<'_>) -> Self::ProduceResultFut {
            todo!()
        }

        fn flush(&self) -> Self::ProduceResultFut {
            todo!()
        }

        fn downcast(&self) -> Self::Downcast {
            self.clone()
        }
    }
}

pub mod rdkafka {}

// -----------------------------------------------------------------------------

fn main() {
    // test producer?
    let cluster = mem::Cluster::builder("mock-1")
        .with_topic(TopicConfig::new("topic-1", 4))
        .build();

    let hasher = |nb_partitions, req: &ProduceRequest| {
        use fasthash::city::hash32_with_seed;
        if let Some(key) = req.key {
            return hash32_with_seed(key, 42) as i32 % nb_partitions as i32;
        }
        0
    };
    let prod = cluster.new_producer(ProducerConfig {
        partitioner: Arc::new(hasher),
    });
}
