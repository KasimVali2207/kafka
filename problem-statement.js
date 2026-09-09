// problem-statement.js - Interactive Simulator for Problem Statement & Message Passing Paradigms
// Paradigms: 1. Sync REST/gRPC | 2. DB Polling | 3. Traditional MQ (RabbitMQ) | 4. Ephemeral Pub/Sub (Redis) | 5. Distributed Log (Kafka)

(function() {
  'use strict';

  // --- Sound Effects Synthesis (Web Audio API) ---
  const PSAudio = {
    ctx: null,
    init() {
      if (!this.ctx) {
        const AudioCtx = window.AudioContext || window.webkitAudioContext;
        if (AudioCtx) this.ctx = new AudioCtx();
      }
    },
    beep(freq, dur = 0.08, type = 'sine', vol = 0.04) {
      if (!this.ctx) return;
      try {
        if (this.ctx.state === 'suspended') this.ctx.resume();
        const osc = this.ctx.createOscillator();
        const gain = this.ctx.createGain();
        osc.type = type;
        osc.frequency.setValueAtTime(freq, this.ctx.currentTime);
        gain.gain.setValueAtTime(vol, this.ctx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.0001, this.ctx.currentTime + dur);
        osc.connect(gain);
        gain.connect(this.ctx.destination);
        osc.start();
        osc.stop(this.ctx.currentTime + dur);
      } catch (e) {
        // silent fallback
      }
    },
    pop() { this.beep(540, 0.05, 'triangle', 0.05); },
    send() { this.beep(440, 0.07, 'sine', 0.04); },
    purge() { this.beep(200, 0.12, 'sawtooth', 0.04); },
    drop() { this.beep(150, 0.2, 'square', 0.06); },
    alarm() { this.beep(180, 0.25, 'square', 0.08); },
    success() { this.beep(780, 0.09, 'sine', 0.05); }
  };

  // --- Paradigm Profiles & Data Models ---
  const PARADIGMS = {
    rest: {
      id: 'rest',
      title: '1. Synchronous Direct REST / gRPC',
      badge: 'Point-to-Point HTTP',
      pillClass: 'pill-blue',
      mediumTitle: 'Direct HTTP / TCP Socket (Zero Intermediary)',
      mediumSub: 'Synchronous blocking thread • HTTP POST /orders',
      coupling: 'Tight (Temporal & Spatial)',
      storageModel: 'None (In-transit memory only)',
      retention: '0s (Ephemeral socket stream)',
      replayability: 'Impossible (0%)',
      defaultLatency: '45ms',
      crashedLatency: '5,000ms (Timeout)',
      description: 'The producer makes a direct HTTP POST request to downstream services. Every consumer must be online simultaneously. If downstream Billing hangs or crashes, the producer thread is locked until it times out.'
    },
    db_polling: {
      id: 'db_polling',
      title: '2. Database Polling / Transactional Outbox',
      badge: 'Shared Relational DB',
      pillClass: 'pill-amber',
      mediumTitle: 'PostgreSQL outbox_events Table',
      mediumSub: 'Periodic cron SELECT ... FOR UPDATE • Lock Contention',
      coupling: 'Medium (Coupled via DB Schema)',
      storageModel: 'Relational DB Rows (Disk + Indexes)',
      retention: 'Until row deletion or table purge',
      replayability: 'Partial (if processed rows kept)',
      defaultLatency: '1,200ms (Polling Interval)',
      crashedLatency: '8,500ms (Deadlocks & Lock Waits)',
      description: 'Producers write events as rows in an `outbox` table. Consumers periodically poll the table with `SELECT * FROM outbox WHERE status="PENDING"`. High polling latency, heavy database lock contention, and high CPU usage under load.'
    },
    rabbitmq: {
      id: 'rabbitmq',
      title: '3. Traditional Message Queue (RabbitMQ / SQS)',
      badge: 'Ephemeral Push Broker',
      pillClass: 'pill-orange',
      mediumTitle: 'RabbitMQ amq.direct Exchange & RAM Queues',
      mediumSub: 'Destructive Read (basic.ack deletes data) • RAM Buffers',
      coupling: 'Loose-Medium (N × M Queue explosion)',
      storageModel: 'RAM Buffers + Ephemeral Mnesia state',
      retention: '0s after ACK (Purged immediately)',
      replayability: 'Impossible (Data deleted on read)',
      defaultLatency: '18ms',
      crashedLatency: 'FROZEN (Backpressure Lock)',
      description: 'Producers send messages to an exchange which pushes to dedicated queues. Once a consumer acknowledges a message, the broker physically purges it from memory. When a consumer stalls, queues bloat and trigger broker backpressure.'
    },
    redis_pubsub: {
      id: 'redis_pubsub',
      title: '4. Ephemeral Pub/Sub (Redis Pub/Sub / SNS)',
      badge: 'Fire-and-Forget Broadcast',
      pillClass: 'pill-rose',
      mediumTitle: 'Redis Channel: order.events',
      mediumSub: 'Fire-and-forget socket broadcast • Zero persistence',
      coupling: 'Loose (Topic-based broadcast)',
      storageModel: 'Zero (Pure in-memory broadcast)',
      retention: '0 milliseconds (No buffer)',
      replayability: 'Impossible (0%)',
      defaultLatency: '3ms',
      crashedLatency: 'SILENT LOSS (100% Dropped)',
      description: 'Producers publish messages to a channel. Redis broadcasts the packet in-memory to currently connected subscribers. If a consumer is offline or disconnected during a network hiccup, the message is permanently lost with zero notification.'
    },
    kafka: {
      id: 'kafka',
      title: '5. Distributed Append-Only Log (Apache Kafka)',
      badge: 'Immutable Commit Log',
      pillClass: 'pill-cyan',
      mediumTitle: 'Kafka Topic Partition Commit Log',
      mediumSub: 'Append-Only Sequential Disk Segments • Zero-Copy I/O',
      coupling: 'Completely Decoupled (Dumb Broker, Smart Consumer)',
      storageModel: 'Immutable disk log segments (OS Page Cache)',
      retention: 'Configurable (e.g. 7 days, 1 year, Infinite)',
      replayability: '100% (Instant Offset Rewind)',
      defaultLatency: '4ms (Zero-Copy DMA)',
      crashedLatency: '4ms (Producer Completely Unaffected)',
      description: 'Producers append events sequentially to an immutable partition log. Consumers independently pull messages and advance their own offset pointers. A dead or slow consumer causes ZERO lag on producers and data can be rewound and replayed anytime.'
    }
  };

  // --- State Engine ---
  const state = {
    currentMethod: 'rest',
    consumerCrashed: false,
    messageCounter: 101,
    isStreaming: false,
    streamTimer: null,
    
    metrics: {
      rest: { sent: 12, acked: 12, failed: 0, lag: 0 },
      db_polling: { sent: 8, acked: 8, failed: 0, lag: 0 },
      rabbitmq: { sent: 15, acked: 15, failed: 0, lag: 0 },
      redis_pubsub: { sent: 20, acked: 18, failed: 2, lag: 0 },
      kafka: { sent: 28, acked: 28, failed: 0, lag: 0 }
    },

    buffers: {
      rest: [],
      db_polling: [
        { id: 'EVT-100', status: 'PROCESSED', time: '10:42:01' }
      ],
      rabbitmq: [
        { id: 'MSG-099', queue: 'billing', status: 'READY' }
      ],
      redis_pubsub: [],
      kafka: [
        { offset: 0, key: 'ACC-101', val: '$500 Deposit' },
        { offset: 1, key: 'ACC-102', val: '$120 ATM' },
        { offset: 2, key: 'ACC-101', val: '$45 POS' }
      ]
    }
  };

  // --- DOM Elements Cache ---
  const DOM = {};

  function cacheDOM() {
    DOM.tabBtns = document.querySelectorAll('.method-tab-btn');
    DOM.titleElem = document.getElementById('pm-method-title');
    DOM.badgeElem = document.getElementById('pm-method-badge');
    DOM.descElem = document.getElementById('pm-method-desc');
    DOM.couplingVal = document.getElementById('pm-val-coupling');
    DOM.storageVal = document.getElementById('pm-val-storage');
    DOM.replayVal = document.getElementById('pm-val-replay');
    DOM.latencyVal = document.getElementById('pm-val-latency');
    DOM.blastVal = document.getElementById('pm-val-blast');
    
    // Intermediary Box
    DOM.mediumBox = document.getElementById('pm-medium-box');
    DOM.mediumTitle = document.getElementById('pm-medium-title');
    DOM.mediumSub = document.getElementById('pm-medium-sub');
    DOM.mediumVisual = document.getElementById('pm-medium-visual');

    // Consumer Nodes
    DOM.consumerBilling = document.getElementById('pm-consumer-billing');
    DOM.billingStatus = document.getElementById('pm-billing-status');
    DOM.billingNote = document.getElementById('pm-billing-note');
    DOM.consumerAnalytics = document.getElementById('pm-consumer-analytics');

    // Controls
    DOM.btnSend = document.getElementById('pm-btn-send');
    DOM.btnCrash = document.getElementById('pm-btn-crash');
    DOM.btnReplay = document.getElementById('pm-btn-replay');
    DOM.btnAuto = document.getElementById('pm-btn-auto');
    DOM.btnReset = document.getElementById('pm-btn-reset');

    // Alert Banner
    DOM.alertBanner = document.getElementById('pm-alert-banner');
    DOM.alertTitle = document.getElementById('pm-alert-title');
    DOM.alertDesc = document.getElementById('pm-alert-desc');
    DOM.alertClose = document.getElementById('pm-alert-close');

    // Audit Log
    DOM.feedList = document.getElementById('pm-feed-list');

    // Modal
    DOM.modal = document.getElementById('pm-replay-modal');
    DOM.modalTitle = document.getElementById('pm-modal-title');
    DOM.modalSubtitle = document.getElementById('pm-modal-sub');
    DOM.modalCode = document.getElementById('pm-modal-code');
    DOM.modalExplanation = document.getElementById('pm-modal-explanation');
    DOM.modalClose = document.getElementById('pm-modal-close');
    DOM.modalJumpKafka = document.getElementById('pm-modal-jump-kafka');
  }

  // --- Audit Feed Helper ---
  function logFeed(type, text) {
    if (!DOM.feedList) return;
    const now = new Date();
    const timeStr = now.toTimeString().split(' ')[0] + '.' + String(now.getMilliseconds()).padStart(3, '0');
    
    const li = document.createElement('li');
    li.className = `pm-feed-item item-${type.toLowerCase()}`;
    
    let icon = '⚡';
    if (type === 'ERROR' || type === 'DROP') icon = '🛑';
    if (type === 'WARN') icon = '⚠️';
    if (type === 'SUCCESS') icon = '✅';
    if (type === 'INFO') icon = 'ℹ️';

    li.innerHTML = `
      <span class="pm-feed-time">${timeStr}</span>
      <span class="pm-feed-tag tag-${type.toLowerCase()}">${icon} ${type}</span>
      <span class="pm-feed-msg">${text}</span>
    `;

    DOM.feedList.insertBefore(li, DOM.feedList.firstChild);
    while (DOM.feedList.children.length > 25) {
      DOM.feedList.removeChild(DOM.feedList.lastChild);
    }
  }

  // --- Alert Banner Helpers ---
  function showAlert(title, desc, isError = true) {
    if (!DOM.alertBanner) return;
    DOM.alertBanner.classList.toggle('is-error', isError);
    DOM.alertBanner.classList.toggle('is-warning', !isError);
    DOM.alertTitle.textContent = title;
    DOM.alertDesc.textContent = desc;
    DOM.alertBanner.classList.add('visible');
  }

  function hideAlert() {
    if (DOM.alertBanner) DOM.alertBanner.classList.remove('visible');
  }

  // --- Switch Paradigm Method ---
  function selectMethod(methodId) {
    if (!PARADIGMS[methodId]) return;
    state.currentMethod = methodId;
    state.consumerCrashed = false;
    hideAlert();
    PSAudio.pop();

    if (DOM.tabBtns) {
      DOM.tabBtns.forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-method') === methodId);
      });
    }

    const p = PARADIGMS[methodId];
    if (DOM.titleElem) DOM.titleElem.textContent = p.title;
    if (DOM.badgeElem) {
      DOM.badgeElem.textContent = p.badge;
      DOM.badgeElem.className = `pm-badge ${p.pillClass}`;
    }
    if (DOM.descElem) DOM.descElem.textContent = p.description;

    if (DOM.couplingVal) DOM.couplingVal.textContent = p.coupling;
    if (DOM.storageVal) DOM.storageVal.textContent = p.storageModel;
    if (DOM.replayVal) DOM.replayVal.textContent = p.replayability;
    if (DOM.latencyVal) DOM.latencyVal.textContent = p.defaultLatency;
    if (DOM.mediumTitle) DOM.mediumTitle.textContent = p.mediumTitle;
    if (DOM.mediumSub) DOM.mediumSub.textContent = p.mediumSub;

    // Reset crash button UI
    if (DOM.btnCrash) {
      DOM.btnCrash.innerHTML = `<span>💥</span> Crash / Slow Billing Service`;
      DOM.btnCrash.classList.remove('btn-active-crash');
    }

    // Reset Consumer UI
    if (DOM.consumerBilling) {
      DOM.consumerBilling.classList.remove('crashed');
      if (DOM.billingStatus) {
        DOM.billingStatus.className = 'pm-node-status status-healthy';
        DOM.billingStatus.textContent = 'ONLINE';
      }
      if (DOM.billingNote) DOM.billingNote.textContent = 'Active & Processing normally';
    }

    renderIntermediary();
    logFeed('INFO', `Switched message passing paradigm to: <strong>${p.title}</strong>. Ready to test.`);
  }

  // --- Render Intermediary / Broker / Log Visual Box ---
  function renderIntermediary() {
    if (!DOM.mediumVisual) return;
    const m = state.currentMethod;

    if (m === 'rest') {
      DOM.mediumVisual.innerHTML = `
        <div class="pm-wire-display">
          <div class="pm-wire-line">
            <span class="pm-wire-pulse"></span>
          </div>
          <div class="pm-wire-details">
            <div class="pm-wire-badge">Direct TCP / TLS Handshake</div>
            <p>No storage buffer. If downstream socket is blocked or down, producer socket blocks directly.</p>
          </div>
        </div>
      `;
    } else if (m === 'db_polling') {
      const rows = state.buffers.db_polling.map(r => `
        <div class="pm-db-row ${r.status === 'LOCKED' ? 'row-locked' : ''}">
          <span class="col-id">${r.id}</span>
          <span class="col-status ${r.status === 'LOCKED' ? 'status-warn' : 'status-ok'}">${r.status}</span>
          <span class="col-time">${r.time || '10:45:00'}</span>
        </div>
      `).join('');

      DOM.mediumVisual.innerHTML = `
        <div class="pm-db-table">
          <div class="pm-table-head">
            <span>event_id (PK)</span>
            <span>processing_status</span>
            <span>created_at</span>
          </div>
          <div class="pm-table-body">
            ${rows}
          </div>
          <div class="pm-table-footer">
            <span>⚠️ Table Lock: ${state.consumerCrashed ? 'ROW LOCK CONTENTION (High CPU)' : 'FREE'}</span>
          </div>
        </div>
      `;
    } else if (m === 'rabbitmq') {
      const items = state.buffers.rabbitmq.map(item => `
        <div class="pm-q-item ${item.status === 'BACKLOG' ? 'item-bloat' : ''}">
          <span>📦 ${item.id}</span>
          <span class="pm-q-item-status">${item.status}</span>
        </div>
      `).join('');

      DOM.mediumVisual.innerHTML = `
        <div class="pm-mq-box">
          <div class="pm-mq-meta">
            <span>Queue: <code>billing.direct.q</code></span>
            <span class="pm-mq-tag ${state.consumerCrashed ? 'tag-danger' : 'tag-safe'}">
              ${state.consumerCrashed ? 'RAM Watermark Alarm' : 'Memory: 24% Normal'}
            </span>
          </div>
          <div class="pm-mq-slots">
            ${items.length ? items : '<span class="empty-text">Queue empty (ACKed messages purged)</span>'}
          </div>
          <div class="pm-mq-footnote">💥 Destructive read: Messages erased upon basic.ack!</div>
        </div>
      `;
    } else if (m === 'redis_pubsub') {
      DOM.mediumVisual.innerHTML = `
        <div class="pm-pubsub-box">
          <div class="pm-pubsub-hub">
            <span class="pm-hub-icon">📡</span>
            <strong>Redis Pub/Sub Dispatcher</strong>
            <span class="pm-hub-note">Channel <code>order.events</code> &bull; Fire-and-Forget</span>
          </div>
          <div class="pm-pubsub-channels">
            <div class="pm-channel-stream">
              <span class="pulse-ring"></span>
              <span>Zero buffer. Unconnected subscribers miss messages permanently!</span>
            </div>
          </div>
        </div>
      `;
    } else if (m === 'kafka') {
      const logs = state.buffers.kafka.map((rec) => `
        <div class="pm-log-segment">
          <span class="pm-log-offset">Offset ${rec.offset}</span>
          <span class="pm-log-key">${rec.key}</span>
          <span class="pm-log-val">${rec.val}</span>
        </div>
      `).join('');

      DOM.mediumVisual.innerHTML = `
        <div class="pm-kafka-box">
          <div class="pm-kafka-meta">
            <span>Topic: <code>bank-tx</code> &bull; Partition 0 (Append-Only Log)</span>
            <span class="pm-kafka-tag">Zero-Copy Page Cache</span>
          </div>
          <div class="pm-kafka-tape">
            ${logs}
            <div class="pm-log-append-point">&lt; Write Head &gt;</div>
          </div>
          <div class="pm-kafka-footnote">✅ Immutable Append-Only Log on Disk &bull; 100% Replayable</div>
        </div>
      `;
    }
  }

  // --- Dispatch Message Handler ---
  function dispatchMessage() {
    PSAudio.init();
    const m = state.currentMethod;
    const msgId = `TX-${state.messageCounter++}`;

    logFeed('INFO', `[${m.toUpperCase()}] Producer initiated dispatch for <strong>${msgId}</strong>...`);
    PSAudio.send();

    if (m === 'rest') {
      if (state.consumerCrashed) {
        PSAudio.alarm();
        if (DOM.latencyVal) DOM.latencyVal.textContent = '5,000ms (504 Timeout)';
        showAlert(
          'HTTP 504 Gateway Timeout!',
          `Downstream Billing Service is crashed or unresponsive. Because REST is synchronous and point-to-point, the producer thread was blocked for 5.0 seconds waiting for a response before terminating with an error!`,
          true
        );
        logFeed('ERROR', `[REST] Producer HTTP POST timeout (5000ms). Connection aborted with <strong>504 Gateway Timeout</strong>!`);
      } else {
        PSAudio.success();
        if (DOM.latencyVal) DOM.latencyVal.textContent = '45ms';
        hideAlert();
        logFeed('SUCCESS', `[REST] HTTP POST ${msgId} &rarr; Billing & Analytics (200 OK, 45ms). Synchronous cycle completed.`);
      }
    } else if (m === 'db_polling') {
      const status = state.consumerCrashed ? 'LOCKED' : 'PROCESSED';
      state.buffers.db_polling.unshift({ id: msgId, status, time: new Date().toTimeString().split(' ')[0] });
      if (state.buffers.db_polling.length > 5) state.buffers.db_polling.pop();
      renderIntermediary();

      if (state.consumerCrashed) {
        PSAudio.alarm();
        if (DOM.latencyVal) DOM.latencyVal.textContent = '8,500ms (Lock Wait)';
        showAlert(
          'Database Lock Contention & Polling Stall!',
          'Consumers are stalled or slow, causing SELECT ... FOR UPDATE transactions to stack up. Lock wait timeouts are spiking and DB CPU is approaching 98%!',
          true
        );
        logFeed('WARN', `[DB POLLING] Inserted ${msgId} into outbox. Downstream consumer lock acquisition delayed by 8500ms!`);
      } else {
        PSAudio.success();
        if (DOM.latencyVal) DOM.latencyVal.textContent = '1,200ms';
        logFeed('SUCCESS', `[DB POLLING] Inserted ${msgId} into outbox table. Polling cron processed row within 1200ms.`);
      }
    } else if (m === 'rabbitmq') {
      if (state.consumerCrashed) {
        state.buffers.rabbitmq.push({ id: msgId, queue: 'billing', status: 'BACKLOG' });
        renderIntermediary();
        PSAudio.alarm();
        if (DOM.latencyVal) DOM.latencyVal.textContent = 'FROZEN (Backpressure)';
        showAlert(
          'RabbitMQ Flow Control / Backpressure Activated!',
          'Unacknowledged messages in billing.direct.q exceeded the broker RAM threshold. RabbitMQ has suspended incoming TCP packets from the producer!',
          true
        );
        logFeed('ERROR', `[RABBITMQ] Backpressure alarm triggered! Queue backlog reached ${state.buffers.rabbitmq.length}. Producer TCP stream throttled.`);
      } else {
        state.buffers.rabbitmq = [];
        renderIntermediary();
        PSAudio.purge();
        if (DOM.latencyVal) DOM.latencyVal.textContent = '18ms';
        hideAlert();
        logFeed('SUCCESS', `[RABBITMQ] ${msgId} delivered &rarr; basic.ack received &rarr; Message physically purged from RAM (destructive read).`);
      }
    } else if (m === 'redis_pubsub') {
      if (state.consumerCrashed) {
        PSAudio.drop();
        if (DOM.latencyVal) DOM.latencyVal.textContent = '0ms (Silently Dropped)';
        showAlert(
          'Message Lost! (0 Connected Subscribers)',
          'Redis Pub/Sub is fire-and-forget. Because the Billing Consumer was disconnected, the message was broadcast into thin air and permanently lost with ZERO delivery acknowledgement!',
          true
        );
        logFeed('DROP', `[REDIS PUB/SUB] Published ${msgId} to channel. Billing Service was offline &rarr; <strong>Message destroyed/dropped permanently</strong>!`);
      } else {
        PSAudio.success();
        if (DOM.latencyVal) DOM.latencyVal.textContent = '3ms';
        hideAlert();
        logFeed('SUCCESS', `[REDIS PUB/SUB] Broadcast ${msgId} to active subscribers (3ms fire-and-forget).`);
      }
    } else if (m === 'kafka') {
      const newOffset = state.buffers.kafka.length;
      state.buffers.kafka.push({
        offset: newOffset,
        key: 'ACC-' + (100 + (newOffset % 4)),
        val: `$${(Math.random() * 400 + 50).toFixed(2)} Tx`
      });
      if (state.buffers.kafka.length > 5) state.buffers.kafka.shift();
      renderIntermediary();

      PSAudio.success();
      if (DOM.latencyVal) DOM.latencyVal.textContent = '4ms (Zero-Copy Append)';

      if (state.consumerCrashed) {
        showAlert(
          'Kafka Resilience: Producer Completely Unaffected!',
          `Even though Billing Service is dead, Kafka wrote offset ${newOffset} to disk in 4ms. The broker continues at multi-gigabyte speeds. When Billing restarts, it will simply resume from offset ${newOffset} with zero lost data!`,
          false
        );
        logFeed('SUCCESS', `[KAFKA] Appended ${msgId} at offset ${newOffset} in 4ms via OS page cache. Dead consumer has 0 impact on producer!`);
      } else {
        hideAlert();
        logFeed('SUCCESS', `[KAFKA] Appended ${msgId} at offset ${newOffset}. Consumer Groups committed offsets independently.`);
      }
    }
  }

  // --- Toggle Crash / Slow Downstream Consumer ---
  function toggleCrash() {
    PSAudio.init();
    state.consumerCrashed = !state.consumerCrashed;
    const m = state.currentMethod;

    if (state.consumerCrashed) {
      PSAudio.alarm();
      DOM.btnCrash.innerHTML = `<span>🟢</span> Restore Billing Service`;
      DOM.btnCrash.classList.add('btn-active-crash');
      DOM.consumerBilling.classList.add('crashed');
      DOM.billingStatus.className = 'pm-node-status status-crashed';
      DOM.billingStatus.textContent = 'CRASHED / OFFLINE';
      DOM.billingNote.textContent = 'Unresponsive • Socket Refused';

      if (m === 'rest') {
        showAlert('Billing Service Crashed!', 'In REST/gRPC, downstream failures immediately break synchronous callers.', true);
        logFeed('ERROR', '[REST] Billing Service went offline! Next HTTP requests will fail with 504 Gateway Timeout.');
      } else if (m === 'db_polling') {
        showAlert('Billing Service Crashed!', 'Pending rows will accumulate in the outbox table and lock wait times will escalate.', true);
        logFeed('WARN', '[DB POLLING] Billing consumer stopped. Outbox table pending count will increase.');
      } else if (m === 'rabbitmq') {
        showAlert('Billing Service Crashed!', 'Unacknowledged messages will stack in RAM and trigger RabbitMQ backpressure.', true);
        logFeed('WARN', '[RABBITMQ] Billing queue consumer disconnected. Queue buffer will accumulate messages.');
      } else if (m === 'redis_pubsub') {
        showAlert('Billing Service Offline!', 'Redis Pub/Sub has NO queues. Any event published right now will be permanently lost.', true);
        logFeed('WARN', '[REDIS PUB/SUB] Subscriber socket dropped. Channel broadcasts will be discarded.');
      } else if (m === 'kafka') {
        showAlert('Billing Service Offline (Kafka Isolated)', 'In Kafka, consumers pull at their own pace. A dead consumer has ZERO blast radius on the cluster or producer!', false);
        logFeed('INFO', '[KAFKA] Billing consumer paused. Kafka broker continues appending at full speed. Offset pointer safely stored.');
      }
    } else {
      PSAudio.success();
      DOM.btnCrash.innerHTML = `<span>💥</span> Crash / Slow Billing Service`;
      DOM.btnCrash.classList.remove('btn-active-crash');
      DOM.consumerBilling.classList.remove('crashed');
      DOM.billingStatus.className = 'pm-node-status status-healthy';
      DOM.billingStatus.textContent = 'ONLINE';
      DOM.billingNote.textContent = 'Active & Processing normally';
      hideAlert();
      logFeed('SUCCESS', 'Billing Service restored and healthy.');
      renderIntermediary();
    }
  }

  // --- Historical Replay Demonstration ---
  function attemptReplay() {
    PSAudio.init();
    const m = state.currentMethod;

    if (m === 'kafka') {
      PSAudio.success();
      showModal(
        'Time-Travel Historical Replay: SUCCESS (Kafka)',
        'Apache Kafka Append-Only Commit Log',
        `$ kafka-consumer-groups.sh --bootstrap-server broker:9092 \\
  --group billing-service \\
  --reset-offsets --to-offset 0 --topic bank-tx --execute

[RESET CONFIRMED]: Offset reset to 0. 
Billing Service is now replaying all historical transactions from the start of the log!`,
        'Because Kafka stores messages sequentially on disk as an immutable append-only commit log, consumers simply reset their offset pointer to rewind time. Audits, ML training, bug recalculations, and disaster recovery are trivial and cause ZERO side effects for other services.'
      );
      logFeed('SUCCESS', '[KAFKA] Consumer offset successfully rewound to 0! Replaying historical records.');
    } else {
      PSAudio.alarm();
      let code = '';
      let explanation = '';

      if (m === 'rest') {
        code = `ERROR [404]: No message store exists.
Direct HTTP requests are transient socket packets. Once the HTTP transaction ends, the payload is gone from the network wire.`;
        explanation = 'Synchronous REST has zero intermediary retention. To re-run old events, you must force upstream API clients to re-send transactions from their origins (impossible for external mobile/web users).';
      } else if (m === 'db_polling') {
        code = `WARN: Outbox table cleanup cron runs every 1 hour (DELETE FROM outbox WHERE status='PROCESSED').
Old records have already been purged to prevent table bloat and index degradation.`;
        explanation = 'While relational databases can persist data, high-throughput systems cannot store millions of historical event rows in an operational OLTP table without tanking performance. Destructive cleanup scripts remove past data.';
      } else if (m === 'rabbitmq') {
        code = `ERROR [404]: Destructive Read Violation.
Messages have already been acknowledged (basic.ack) by consumer and permanently destroyed from RabbitMQ memory. Offset pointers do not exist.`;
        explanation = 'Traditional message brokers are designed for transient queueing, not event storage. Once acknowledged, data is deleted. You cannot rewind a RabbitMQ queue.';
      } else if (m === 'redis_pubsub') {
        code = `ERROR: Channel 'order.events' buffer capacity = 0 bytes.
Redis Pub/Sub maintains NO message history. Zero bytes are kept on disk or RAM.`;
        explanation = 'Redis Pub/Sub is purely fire-and-forget. There is zero historical replayability.';
      }

      showModal(
        `Operation Denied: Replay Impossible (${PARADIGMS[m].badge})`,
        PARADIGMS[m].title,
        code,
        explanation
      );
      logFeed('ERROR', `[${m.toUpperCase()}] Historical replay failed: Paradigm does not support offset-based time travel!`);
    }
  }

  // --- Modal Helpers ---
  function showModal(title, sub, code, explanation) {
    if (!DOM.modal) return;
    DOM.modalTitle.textContent = title;
    DOM.modalSubtitle.textContent = sub;
    DOM.modalCode.textContent = code;
    DOM.modalExplanation.textContent = explanation;
    DOM.modal.classList.add('active');
  }

  function hideModal() {
    if (DOM.modal) DOM.modal.classList.remove('active');
  }

  // --- Auto Traffic Streamer ---
  function toggleAuto() {
    state.isStreaming = !state.isStreaming;
    if (state.isStreaming) {
      DOM.btnAuto.innerHTML = `<span>⏸</span> Live Traffic: ACTIVE`;
      DOM.btnAuto.classList.add('btn-active-auto');
      dispatchMessage();
      state.streamTimer = setInterval(() => {
        dispatchMessage();
      }, 1800);
    } else {
      DOM.btnAuto.innerHTML = `<span>▶</span> Live Traffic: OFF`;
      DOM.btnAuto.classList.remove('btn-active-auto');
      clearInterval(state.streamTimer);
      state.streamTimer = null;
    }
  }

  // --- Reset Simulation ---
  function resetSimulation() {
    PSAudio.init();
    PSAudio.pop();
    if (state.isStreaming) toggleAuto();
    state.consumerCrashed = false;
    state.messageCounter = 101;
    state.buffers.db_polling = [{ id: 'EVT-100', status: 'PROCESSED', time: '10:42:01' }];
    state.buffers.rabbitmq = [];
    state.buffers.kafka = [
      { offset: 0, key: 'ACC-101', val: '$500 Deposit' },
      { offset: 1, key: 'ACC-102', val: '$120 ATM' },
      { offset: 2, key: 'ACC-101', val: '$45 POS' }
    ];
    selectMethod(state.currentMethod);
    logFeed('INFO', 'Reset simulation state to defaults.');
  }

  // --- Event Listeners Initialization ---
  function initListeners() {
    DOM.tabBtns.forEach(btn => {
      btn.addEventListener('click', () => {
        const method = btn.getAttribute('data-method');
        selectMethod(method);
      });
    });

    if (DOM.btnSend) DOM.btnSend.addEventListener('click', dispatchMessage);
    if (DOM.btnCrash) DOM.btnCrash.addEventListener('click', toggleCrash);
    if (DOM.btnReplay) DOM.btnReplay.addEventListener('click', attemptReplay);
    if (DOM.btnAuto) DOM.btnAuto.addEventListener('click', toggleAuto);
    if (DOM.btnReset) DOM.btnReset.addEventListener('click', resetSimulation);
    if (DOM.alertClose) DOM.alertClose.addEventListener('click', hideAlert);

    if (DOM.modalClose) DOM.modalClose.addEventListener('click', hideModal);
    if (DOM.modalJumpKafka) {
      DOM.modalJumpKafka.addEventListener('click', () => {
        hideModal();
        const kafkaTabBtn = document.getElementById('tab-btn-kafka');
        if (kafkaTabBtn) kafkaTabBtn.click();
      });
    }

    // Close modal on click outside or Escape
    window.addEventListener('click', (e) => {
      if (DOM.modal && e.target === DOM.modal) hideModal();
    });
    window.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && DOM.modal && DOM.modal.classList.contains('active')) hideModal();
    });
  }

  // --- Bootstrap ---
  function init() {
    cacheDOM();
    initListeners();
    selectMethod('rest');
    logFeed('INFO', 'Problem Statement & Message Passing visualizer ready. Choose a paradigm above to explore.');
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
