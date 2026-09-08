// Kafka Educational Data & Code Blueprints for Banking Architecture

const KAFKA_DATA = {
  concepts: [
    {
      id: "why-kafka",
      title: "Why Kafka in Banking & FinTech?",
      icon: "🏦",
      tagline: "Event-Driven Backbone for High-Throughput, Fault-Tolerant Financial Systems",
      summary: "Traditional relational databases and legacy request-response APIs struggle with high-frequency transactions, peak bursts (Black Friday / Payroll days), and complex distributed fan-out. Kafka provides an immutable, ordered, replayable event log that decouples transaction ingestion from processing systems.",
      keyPoints: [
        {
          title: "Immutable Distributed Commit Log",
          desc: "Every transaction (debit, credit, card swipe) is appended sequentially to an immutable log. Events can never be altered or deleted arbitrarily, fulfilling strict banking auditability (SOX, PCI-DSS)."
        },
        {
          title: "Massive Throughput & Zero-Copy I/O",
          desc: "Kafka leverages OS page cache and OS-level zero-copy data transfer (Linux sendfile syscall) to stream millions of financial events per second with sub-millisecond latency."
        },
        {
          title: "Decoupled 1-to-Many Consumer Groups",
          desc: "A single payment event on payments-incoming is independently consumed by Fraud Detection (real-time ML), Core Ledger (balance update), Push Notifications (SMS/Email), and Data Lake (Snowflake/BigQuery) without impacting the payment gateway."
        },
        {
          title: "Time-Travel & Event Replay",
          desc: "Consumers track their own offset position. If an audit is required or a downstream service crashes for 4 hours, it can simply resume or rewind its offset to replay historical transactions without data loss."
        }
      ]
    },
    {
      id: "topics-partitions",
      title: "Topics, Partitions & Key-Based Ordering",
      icon: "⚡",
      tagline: "The Secret to Infinite Scalability and Strict Per-Account Ordering",
      summary: "A Topic is a logical category of events (e.g., tx-settled). To scale horizontally, a topic is subdivided into Partitions distributed across multiple brokers in the cluster.",
      keyPoints: [
        {
          title: "Partitioning by Message Key (Account ID)",
          desc: "By setting the message key to account_id (e.g., ACC-4910), Kafka's default partitioner hashes the key (MurmurHash2(key) % num_partitions) so that all transactions for that specific account always land in the EXACT SAME partition in strict chronological order."
        },
        {
          title: "Parallelism & Scalability",
          desc: "Each partition can be read concurrently by a separate consumer instance in a consumer group. If you have 12 partitions, you can run up to 12 parallel consumers processing transactions simultaneously."
        },
        {
          title: "Log Segments & High Watermark",
          desc: "Partitions are saved to disk as rolling log segment files (.log) with index files (.index, .timeindex). The High Watermark (HW) is the highest offset replicated to all In-Sync Replicas (ISR), ensuring consumers only read safely persisted data."
        }
      ]
    },
    {
      id: "replication-kraft",
      title: "Replication, ISR & KRaft Consensus",
      icon: "🛡️",
      tagline: "High Availability with Zero Single-Point-of-Failure",
      summary: "Financial systems cannot afford downtime or lost transactions. Kafka replicates each partition across multiple brokers (e.g., Replication Factor = 3) and manages cluster state with the Raft-based KRaft consensus engine.",
      keyPoints: [
        {
          title: "Partition Leader & Follower Replicas",
          desc: "For every partition, one broker acts as the Leader (handles all read/write traffic). Follower brokers replicate the leader's log. If the leader crashes, an In-Sync Replica (ISR) is immediately promoted to new leader."
        },
        {
          title: "acks=all and min.insync.replicas",
          desc: "In banking, producers configure acks=all (or -1) and topics set min.insync.replicas=2. A transaction is only acknowledged as committed once the leader and at least one replica confirm disk write."
        },
        {
          title: "KRaft: Built-in Event Metadata Quorum",
          desc: "Modern Kafka replaces ZooKeeper with KRaft (Kafka Raft Metadata mode). A quorum of Controller nodes manages cluster metadata as an internal Kafka topic (@metadata), delivering sub-second controller failovers and millions of partitions."
        }
      ]
    },
    {
      id: "guarantees-eos",
      title: "Delivery Guarantees & Exactly-Once Semantics (EOS)",
      icon: "🎯",
      tagline: "Eliminating Duplicate Debits and Phantom Balances",
      summary: "In finance, double-charging a customer or losing a deposit is catastrophic. Kafka provides Exactly-Once Semantics across produce-stream-consume pipelines.",
      keyPoints: [
        {
          title: "At-Least-Once vs At-Most-Once",
          desc: "At-most-once commits offsets before processing (risk of lost transactions). At-least-once commits offsets after processing (risk of duplicate processing on consumer crash)."
        },
        {
          title: "Idempotent Producer",
          desc: "With enable.idempotence=true, the broker assigns a Producer ID (PID) and sequence numbers to every message batch. If network retries resend a batch, the broker recognizes duplicates and discards them seamlessly."
        },
        {
          title: "Kafka Transactions (2-Phase Commit)",
          desc: "With transactional.id and Kafka Streams processing.guarantee=exactly_once_v2, Kafka coordinates atomic writes across multiple input partitions and output topics, ensuring state updates and offset commits succeed together or abort completely."
        }
      ]
    }
  ],

  codeSnippets: {
    java: [
      {
        title: "Spring Boot Banking Producer (Idempotent & Resilient)",
        filename: "BankingTransactionProducer.java",
        code: `@Service
@RequiredArgsConstructor
@Slf4j
public class BankingTransactionProducer {

    private final KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    public CompletableFuture<SendResult<String, TransactionEvent>> sendPayment(TransactionEvent event) {
        log.info("Dispatching payment event: id={}, account={}", event.getTransactionId(), event.getAccountId());
        
        // Keying by accountId ensures strict chronological partition ordering for the account
        ProducerRecord<String, TransactionEvent> record = new ProducerRecord<>(
            "payments-incoming",
            event.getAccountId(), 
            event
        );
        record.headers().add("X-Trace-Id", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("X-Source-App", "MOBILE_BANKING_API".getBytes(StandardCharsets.UTF_8));

        return kafkaTemplate.send(record)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    RecordMetadata metadata = result.getRecordMetadata();
                    log.info("Transaction published to topic={}, partition={}, offset={}", 
                        metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("CRITICAL: Failed to publish transaction {}", event.getTransactionId(), ex);
                    // Trigger compensatory workflow or alert on-call
                }
            });
    }
}`
      },
      {
        title: "Spring Boot Production Producer Configuration",
        filename: "application.yml",
        code: `spring:
  kafka:
    bootstrap-servers: broker-1:9092,broker-2:9092,broker-3:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
      # Critical Banking Settings for Zero Data Loss & Exactly Once:
      acks: all                           # Wait for all In-Sync Replicas
      retries: 2147483647                 # Retry indefinitely on transient failures
      properties:
        enable.idempotence: true          # Prevent duplicate records on network retry
        max.in.flight.requests.per.connection: 5 # Safe with idempotence enabled
        compression.type: zstd            # High compression ratio & low CPU latency
        linger.ms: 5                      # Micro-batching to maximize throughput
        batch.size: 65536                 # 64KB batch size
        schema.registry.url: http://schema-registry:8081`
      },
      {
        title: "Fraud Detection & Ledger Consumers with DLQ",
        filename: "BankingConsumers.java",
        code: `@Component
@Slf4j
public class BankingConsumers {

    @Autowired
    private FraudScoringService fraudService;
    @Autowired
    private CoreLedgerService ledgerService;

    // Consumer Group 1: Real-Time Fraud Detection Engine
    @KafkaListener(
        topics = "payments-incoming", 
        groupId = "fraud-detection-service",
        concurrency = "3" // 1 thread per partition
    )
    public void detectFraud(ConsumerRecord<String, TransactionEvent> record, Acknowledgment ack) {
        TransactionEvent tx = record.value();
        double riskScore = fraudService.evaluateRisk(tx);
        
        if (riskScore > 0.85) {
            log.warn("🚨 HIGH FRAUD RISK ({}) on account {}", riskScore, tx.getAccountId());
            fraudService.triggerAccountFreeze(tx);
        }
        ack.acknowledge(); // Manual commit after processing
    }

    // Consumer Group 2: Core Banking Ledger (Idempotent Settlement)
    @KafkaListener(
        topics = "payments-incoming", 
        groupId = "core-banking-ledger",
        errorHandler = "deadLetterPublishingErrorHandler"
    )
    public void settleTransaction(TransactionEvent tx, Acknowledgment ack) {
        ledgerService.processIdempotentDebitCredit(tx);
        ack.acknowledge();
    }
}`
      }
    ],

    python: [
      {
        title: "Python Confluent-Kafka Producer (Fast & Reliable)",
        filename: "producer.py",
        code: `import json
import uuid
import time
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed for key {msg.key()}: {err}")
    else:
        print(f"✅ Tx committed to {msg.topic()} [Partition: {msg.partition()}] at offset {msg.offset()}")

conf = {
    'bootstrap.servers': 'broker1:9092,broker2:9092,broker3:9092',
    'client.id': 'fintech-payment-gateway',
    'acks': 'all',                  # Strongest durability
    'enable.idempotence': True,     # Exactly-once write semantics
    'compression.type': 'snappy',
    'queue.buffering.max.messages': 500000,
}

producer = Producer(conf)

def publish_banking_transaction(account_id, amount, tx_type, merchant):
    tx_payload = {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "amount": amount,
        "type": tx_type,
        "merchant": merchant,
        "timestamp_utc": int(time.time() * 1000)
    }
    
    # Key = account_id ensures strict in-order processing per user account
    producer.produce(
        topic='payments-incoming',
        key=account_id.encode('utf-8'),
        value=json.dumps(tx_payload).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.poll(0)

# Example usage
publish_banking_transaction("ACC-884102", 450.00, "WIRE_TRANSFER", "Standard Chartered NYC")
producer.flush()`
      },
      {
        title: "Python Real-Time Fraud Stream Consumer",
        filename: "fraud_consumer.py",
        code: `from confluent_kafka import Consumer, KafkaError
import json

conf = {
    'bootstrap.servers': 'broker1:9092,broker2:9092',
    'group.id': 'fraud-detection-engine-py',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,     # Manual commit after safe processing
    'isolation.level': 'read_committed' # Read only committed transactional messages
}

consumer = Consumer(conf)
consumer.subscribe(['payments-incoming'])

print("🔍 Real-Time Fraud Detection Engine listening on 'payments-incoming'...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Consumer error: {msg.error()}")
            break

        tx = json.loads(msg.value().decode('utf-8'))
        
        # Real-time Fraud Heuristic Evaluation
        if tx['amount'] > 10000 and "CRYPTO" in tx['merchant'].upper():
            print(f"🚨 FRAUD ALERT! Large unverified crypto transfer on {tx['account_id']}")
        
        # Commit offset after successful scoring
        consumer.commit(msg, asynchronous=False)

finally:
    consumer.close()`
      }
    ],

    nodejs: [
      {
        title: "Node.js KafkaJS Banking Pipeline",
        filename: "banking-service.js",
        code: `const { Kafka, CompressionTypes, logLevel } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'fintech-node-gateway',
  brokers: ['broker-1:9092', 'broker-2:9092'],
  logLevel: logLevel.INFO,
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
});

// Resilient Producer
const producer = kafka.producer({
  idempotent: true,
  maxInFlightRequests: 1
});

async function runPaymentGateway() {
  await producer.connect();

  const transaction = {
    transactionId: 'TX-902148',
    accountId: 'ACC-55201',
    amount: 1250.50,
    currency: 'USD',
    recipient: 'GLOBAL_SUPPLIER_INC'
  };

  await producer.send({
    topic: 'payments-incoming',
    compression: CompressionTypes.GZIP,
    messages: [
      {
        key: transaction.accountId, // Guarantees same partition ordering
        value: JSON.stringify(transaction),
        headers: { 'correlation-id': 'corr-abc-123' }
      }
    ]
  });

  console.log('💳 Payment dispatched to Kafka successfully!');
}

runPaymentGateway().catch(console.error);`
      }
    ]
  },

  quizQuestions: [
    {
      question: "Why do we use the Customer's Account ID as the message key when publishing payment events in Kafka?",
      options: [
        "To encrypt the transaction payload during transit",
        "To guarantee that all transactions for that account land in the same partition and are processed in exact chronological order",
        "To broadcast the transaction to all brokers simultaneously",
        "To bypass the consumer group rebalancing protocol"
      ],
      correctIndex: 1,
      explanation: "Kafka hashes the message key (MurmurHash2(key) % num_partitions). By using Account ID as the key, every debit, credit, or dispute for that account arrives at the same partition, preserving strict chronological ordering."
    },
    {
      question: "In a critical banking system, what producer configuration prevents lost messages during broker failures?",
      options: [
        "acks=0 and compression=none",
        "acks=1 and max.in.flight.requests=10",
        "acks=all (or -1) combined with min.insync.replicas=2 and enable.idempotence=true",
        "auto.offset.reset=latest and buffer.memory=1024"
      ],
      correctIndex: 2,
      explanation: "Setting acks=all ensures the leader broker waits until all In-Sync Replicas acknowledge writing to their disk logs. Combined with min.insync.replicas >= 2 and idempotence=true, zero data loss is guaranteed even if the leader crashes."
    },
    {
      question: "If a topic has 3 partitions, and a consumer group has 5 active consumer instances, how many consumers will be idle?",
      options: [
        "0 (Partitions are shared among all 5)",
        "2 consumers will be idle",
        "All 5 consumers will fail with a RebalanceException",
        "1 consumer will be idle"
      ],
      correctIndex: 1,
      explanation: "In Kafka, a single partition can only be assigned to at most ONE consumer within a given consumer group at any time. With 3 partitions, only 3 consumers will actively read, leaving 2 as hot standbys."
    },
    {
      question: "What happens when a 'Poison Pill' (corrupted/unparseable transaction) reaches a consumer in a standard pipeline?",
      options: [
        "Kafka automatically deletes the entire partition",
        "The consumer crashes in an infinite retry loop, stalling partition processing until handled via a Dead Letter Queue (DLQ)",
        "The broker translates the message to valid JSON automatically",
        "The consumer group automatically adds 2 more partitions"
      ],
      correctIndex: 1,
      explanation: "A poison pill causes deserialization failure. Without proper error handling (routing the bad record to a Dead Letter Queue / tx-dlq), the consumer restarts, polls the same bad offset again, and stalls the pipeline."
    },
    {
      question: "What is KRaft in modern Apache Kafka?",
      options: [
        "A Java UI framework for monitoring brokers",
        "A consensus protocol (Kafka Raft) that manages cluster metadata internally, completely eliminating the need for Apache ZooKeeper",
        "A cloud storage driver for archiving logs to AWS S3",
        "An AI plugin for predicting partition skew"
      ],
      correctIndex: 1,
      explanation: "KRaft (KIP-500) replaces ZooKeeper with an event-driven Raft consensus quorum running inside Kafka itself, improving scalability up to millions of partitions with instant failover."
    }
  ]
};
