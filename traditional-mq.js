// traditional-mq.js - Interactive Simulator for Traditional MQ (RabbitMQ) vs Apache Kafka
// Demonstrating: Tightly Coupled Architecture, Destructive Reads, Queue Bloat & Producer Backpressure Cascades

(function() {
  'use strict';

  // --- Sound Effects Synthesis (Reusing Web Audio API) ---
  const TMQAudio = {
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
        // Audio error silent fallback
      }
    },
    pop() { this.beep(520, 0.05, 'triangle', 0.05); },
    purge() { this.beep(220, 0.12, 'sawtooth', 0.04); },
    alarm() { this.beep(180, 0.25, 'square', 0.08); },
    success() { this.beep(780, 0.09, 'sine', 0.05); }
  };

  // --- Simulation State ---
  const state = {
    billingCrashed: false,
    fraudServiceAdded: false,
    backpressureActive: false,
    memoryPercent: 24,
    orderCounter: 105,
    autoInterval: null,
    isAutoRunning: false,
    
    // Queues buffer data
    queues: {
      orders: {
        id: 'q-orders',
        name: 'orders.process.queue',
        consumerId: 'c-order',
        consumerName: 'Order Fulfilment Service',
        items: ['ORD-101', 'ORD-102'],
        max: 6,
        totalAcked: 18
      },
      billing: {
        id: 'q-billing',
        name: 'billing.invoice.queue',
        consumerId: 'c-billing',
        consumerName: 'Billing & Invoice Service',
        items: ['ORD-102'],
        max: 6,
        totalAcked: 12
      },
      notify: {
        id: 'q-notify',
        name: 'notifications.sms.queue',
        consumerId: 'c-notify',
        consumerName: 'SMS / Email Notification Service',
        items: ['ORD-102'],
        max: 6,
        totalAcked: 22
      },
      fraud: {
        id: 'q-fraud',
        name: 'fraud.detection.queue',
        consumerId: 'c-fraud',
        consumerName: 'ML Fraud Detection AI',
        items: [],
        max: 6,
        totalAcked: 0
      }
    },

    // Total metrics
    totalPublished: 42,
    totalPurged: 52
  };

  // --- Tab Navigation Setup ---
  function initTabNavigation() {
    const tabBtns = document.querySelectorAll('.nav-tab-btn');
    const viewSections = document.querySelectorAll('.view-section');

    tabBtns.forEach(btn => {
      btn.addEventListener('click', () => {
        const targetViewId = btn.getAttribute('data-view');
        switchView(targetViewId);
      });
    });

    // Handle "See Kafka Solution" jump buttons
    document.querySelectorAll('.btn-jump-to-kafka').forEach(el => {
      el.addEventListener('click', (e) => {
        e.preventDefault();
        switchView('view-kafka-simulator');
      });
    });

    // Handle "Show Syntax" jump buttons
    document.querySelectorAll('.btn-jump-to-syntax').forEach(el => {
      el.addEventListener('click', (e) => {
        e.preventDefault();
        switchView('view-syntax-cheatsheet');
      });
    });
  }

  function switchView(viewId) {
    const tabBtns = document.querySelectorAll('.nav-tab-btn');
    const viewSections = document.querySelectorAll('.view-section');

    tabBtns.forEach(btn => {
      const match = btn.getAttribute('data-view') === viewId;
      btn.classList.toggle('active', match);
    });

    viewSections.forEach(section => {
      const match = section.id === viewId;
      section.classList.toggle('active', match);
    });

    // If switching to Kafka Simulator, trigger canvas resize and redraw
    if (viewId === 'view-kafka-simulator') {
      window.dispatchEvent(new Event('resize'));
      if (typeof window.resizeKafkaCanvas === 'function') {
        window.resizeKafkaCanvas();
      }
    }

    // If switching to Syntax Cheat Sheet, auto-trigger lazy load if needed
    if (viewId === 'view-syntax-cheatsheet') {
      const loadBtn = document.getElementById('load-syntax-btn');
      if (loadBtn) {
        loadBtn.click();
      }
    }

    // Smooth scroll to top of main view container
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  // --- Traditional MQ Visualizer Core ---
  const DOM = {};

  function cacheDOMElements() {
    DOM.btnSend = document.getElementById('tmq-btn-send');
    DOM.btnCrash = document.getElementById('tmq-btn-crash');
    DOM.btnAddConsumer = document.getElementById('tmq-btn-add-consumer');
    DOM.btnReplay = document.getElementById('tmq-btn-replay');
    DOM.btnReset = document.getElementById('tmq-btn-reset');
    DOM.btnAuto = document.getElementById('tmq-btn-auto');

    DOM.gaugeMemoryFill = document.getElementById('tmq-gauge-fill');
    DOM.metricMemoryText = document.getElementById('tmq-metric-memory');
    DOM.metricProducerStatus = document.getElementById('tmq-metric-producer-status');
    DOM.metricRetention = document.getElementById('tmq-metric-retention');
    DOM.metricCoupling = document.getElementById('tmq-metric-coupling');

    DOM.alertBanner = document.getElementById('tmq-alert-banner');
    DOM.alertTitle = document.getElementById('tmq-alert-title');
    DOM.alertDesc = document.getElementById('tmq-alert-desc');
    DOM.btnCloseAlert = document.getElementById('tmq-close-alert');

    // Queues
    DOM.qOrdersBox = document.getElementById('tmq-q-orders');
    DOM.qBillingBox = document.getElementById('tmq-q-billing');
    DOM.qNotifyBox = document.getElementById('tmq-q-notify');
    DOM.qFraudBox = document.getElementById('tmq-q-fraud');

    // Consumers
    DOM.cOrderNode = document.getElementById('tmq-node-c-order');
    DOM.cBillingNode = document.getElementById('tmq-node-c-billing');
    DOM.cNotifyNode = document.getElementById('tmq-node-c-notify');
    DOM.cFraudNode = document.getElementById('tmq-node-c-fraud');

    // Producers
    DOM.prodNodes = document.querySelectorAll('.tmq-prod-node');

    // Replay Modal / Toast
    DOM.replayModal = document.getElementById('tmq-replay-modal');
    DOM.btnCloseReplayModal = document.getElementById('tmq-close-replay-modal');

    // Live Event Log
    DOM.feedList = document.getElementById('tmq-feed-list');
  }

  function renderQueues() {
    // Render orders queue
    renderQueueItems('orders', DOM.qOrdersBox);
    renderQueueItems('billing', DOM.qBillingBox);
    renderQueueItems('notify', DOM.qNotifyBox);
    if (state.fraudServiceAdded) {
      if (DOM.qFraudBox) DOM.qFraudBox.style.display = 'flex';
      if (DOM.cFraudNode) DOM.cFraudNode.style.display = 'flex';
      renderQueueItems('fraud', DOM.qFraudBox);
    } else {
      if (DOM.qFraudBox) DOM.qFraudBox.style.display = 'none';
      if (DOM.cFraudNode) DOM.cFraudNode.style.display = 'none';
    }

    // Update memory gauge & backpressure status
    updateBrokerStatus();
  }

  function renderQueueItems(qKey, containerEl) {
    if (!containerEl) return;
    const qData = state.queues[qKey];
    const slotsEl = containerEl.querySelector('.tmq-queue-slots');
    const countEl = containerEl.querySelector('.tmq-q-count');
    const badgeEl = containerEl.querySelector('.tmq-q-badge');

    if (countEl) {
      countEl.textContent = `${qData.items.length}/${qData.max}`;
    }

    if (slotsEl) {
      slotsEl.innerHTML = '';
      if (qData.items.length === 0) {
        slotsEl.innerHTML = `<span class="tmq-empty-q-tag">Queue Empty (Awaiting Orders)</span>`;
      } else {
        qData.items.forEach((item, idx) => {
          const itemDiv = document.createElement('div');
          itemDiv.className = 'tmq-msg-pill';
          itemDiv.innerHTML = `<span>✉️ ${item}</span>`;
          slotsEl.appendChild(itemDiv);
        });
      }
    }

    if (badgeEl) {
      if (qData.items.length >= qData.max) {
        badgeEl.textContent = 'QUEUE FULL';
        badgeEl.className = 'tmq-q-badge badge-danger';
      } else if (qData.items.length >= 4) {
        badgeEl.textContent = 'BUFFER HIGH';
        badgeEl.className = 'tmq-q-badge badge-warning';
      } else {
        badgeEl.textContent = 'DRAINING';
        badgeEl.className = 'tmq-q-badge badge-healthy';
      }
    }
  }

  function updateBrokerStatus() {
    // Calculate memory based on pending messages in all queues
    const billingCount = state.queues.billing.items.length;
    const ordersCount = state.queues.orders.items.length;
    const notifyCount = state.queues.notify.items.length;
    const fraudCount = state.fraudServiceAdded ? state.queues.fraud.items.length : 0;
    const totalPending = billingCount + ordersCount + notifyCount + fraudCount;

    // Baseline memory is 20% + 12% per pending message
    let calculatedMem = Math.min(100, Math.round(20 + totalPending * 12));

    // If billing queue is full (>=5), memory jumps to 95%+
    if (billingCount >= 5) {
      calculatedMem = Math.max(92, calculatedMem);
    }

    state.memoryPercent = calculatedMem;

    // Check Backpressure
    if (calculatedMem >= 88 || billingCount >= state.queues.billing.max) {
      state.backpressureActive = true;
    } else {
      state.backpressureActive = false;
    }

    // Update HUD
    if (DOM.gaugeMemoryFill) {
      DOM.gaugeMemoryFill.style.width = `${state.memoryPercent}%`;
      if (state.memoryPercent >= 85) {
        DOM.gaugeMemoryFill.style.background = 'linear-gradient(90deg, #F59E0B, #EF4444)';
      } else if (state.memoryPercent >= 50) {
        DOM.gaugeMemoryFill.style.background = 'linear-gradient(90deg, #06B6D4, #F59E0B)';
      } else {
        DOM.gaugeMemoryFill.style.background = 'linear-gradient(90deg, #10B981, #06B6D4)';
      }
    }

    if (DOM.metricMemoryText) {
      DOM.metricMemoryText.textContent = `${state.memoryPercent}% ${state.memoryPercent >= 85 ? '(CRITICAL BUFFER)' : '(Normal)'}`;
      DOM.metricMemoryText.style.color = state.memoryPercent >= 85 ? '#EF4444' : (state.memoryPercent >= 50 ? '#F59E0B' : '#10B981');
    }

    if (DOM.metricProducerStatus) {
      if (state.backpressureActive) {
        DOM.metricProducerStatus.innerHTML = `<span class="tmq-dot dot-red"></span> 🛑 BLOCKED: BACKPRESSURE`;
        DOM.metricProducerStatus.style.color = '#EF4444';
        showAlert(
          '🛑 RabbitMQ Backpressure Triggered! Producers Frozen',
          'Because the Billing Service crashed, unacknowledged messages filled up the queue buffer and exceeded the RabbitMQ High Memory Watermark. The broker has blocked all incoming TCP connections, freezing Checkout Web App & Mobile Gateway from taking new orders!'
        );
      } else {
        DOM.metricProducerStatus.innerHTML = `<span class="tmq-dot dot-green"></span> 🟢 Ingesting Normally`;
        DOM.metricProducerStatus.style.color = '#10B981';
      }
    }

    // Update Producer UI cards
    DOM.prodNodes.forEach(node => {
      node.classList.toggle('prod-blocked', state.backpressureActive);
      const tag = node.querySelector('.tmq-prod-status-tag');
      if (tag) {
        if (state.backpressureActive) {
          tag.textContent = 'BLOCKED (429)';
          tag.className = 'tmq-prod-status-tag tag-blocked';
        } else {
          tag.textContent = 'ONLINE';
          tag.className = 'tmq-prod-status-tag tag-online';
        }
      }
    });

    // Update Billing Consumer Node UI
    if (DOM.cBillingNode) {
      DOM.cBillingNode.classList.toggle('consumer-crashed', state.billingCrashed);
      const statusBadge = DOM.cBillingNode.querySelector('.tmq-c-status');
      if (statusBadge) {
        if (state.billingCrashed) {
          statusBadge.textContent = '💥 CRASHED / STALLED';
          statusBadge.className = 'tmq-c-status status-crashed';
        } else {
          statusBadge.textContent = 'ONLINE • 35ms ACK';
          statusBadge.className = 'tmq-c-status status-online';
        }
      }
    }

    // Coupling metric
    if (DOM.metricCoupling) {
      const activeConsumersCount = 3 + (state.fraudServiceAdded ? 1 : 0);
      DOM.metricCoupling.textContent = `O(3 × ${activeConsumersCount}) = ${3 * activeConsumersCount} Tight Links`;
    }
  }

  function showAlert(title, desc) {
    if (!DOM.alertBanner) return;
    DOM.alertTitle.textContent = title;
    DOM.alertDesc.textContent = desc;
    DOM.alertBanner.classList.add('active');
    TMQAudio.alarm();
  }

  function hideAlert() {
    if (DOM.alertBanner) DOM.alertBanner.classList.remove('active');
  }

  // --- Dispatch Transaction in Traditional MQ ---
  function dispatchMessage() {
    TMQAudio.init();

    if (state.backpressureActive) {
      showAlert(
        'Cannot Send: Producers Blocked by Backpressure!',
        'RabbitMQ refuses to accept new messages while memory usage is critical. Please restart/recover the Billing Service or reset the queue simulation.'
      );
      TMQAudio.alarm();
      return;
    }

    state.orderCounter++;
    const orderId = `ORD-${state.orderCounter}`;
    const amount = (Math.random() * 400 + 49).toFixed(2);
    const orderLabel = `${orderId} ($${amount})`;

    // Flash a random producer
    const randProdIndex = Math.floor(Math.random() * DOM.prodNodes.length);
    const prodNode = DOM.prodNodes[randProdIndex];
    if (prodNode) {
      prodNode.classList.add('tmq-flash');
      setTimeout(() => prodNode.classList.remove('tmq-flash'), 300);
    }

    TMQAudio.pop();
    logFeed('PUBLISH', `Producer <code>${prodNode ? prodNode.getAttribute('data-name') : 'Checkout API'}</code> sent <strong>${orderLabel}</strong> to exchange <code>amq.direct</code>`);

    // In Traditional MQ (RabbitMQ exchange):
    // The message is duplicated into each bound queue!
    setTimeout(() => {
      // 1. Enqueue to orders.process.queue
      if (state.queues.orders.items.length < state.queues.orders.max) {
        state.queues.orders.items.push(orderId);
      }
      
      // 2. Enqueue to billing.invoice.queue
      if (state.queues.billing.items.length < state.queues.billing.max) {
        state.queues.billing.items.push(orderId);
      }

      // 3. Enqueue to notifications.sms.queue
      if (state.queues.notify.items.length < state.queues.notify.max) {
        state.queues.notify.items.push(orderId);
      }

      // 4. If fraud service added, duplicate to fraud queue as well!
      if (state.fraudServiceAdded && state.queues.fraud.items.length < state.queues.fraud.max) {
        state.queues.fraud.items.push(orderId);
      }

      renderQueues();

      // Trigger asynchronous consumer pulls with Destructive Read (Ack & Delete)
      processConsumerPulls(orderId);

    }, 350);
  }

  function processConsumerPulls(orderId) {
    // 1. Order Fulfilment Service pulls & Acks
    setTimeout(() => {
      const idx = state.queues.orders.items.indexOf(orderId);
      if (idx !== -1) {
        state.queues.orders.items.splice(idx, 1);
        state.queues.orders.totalAcked++;
        flashConsumerAck(DOM.cOrderNode, 'Order Fulfilment Svc', orderId);
        renderQueues();
      }
    }, 450);

    // 2. Notifications Service pulls & Acks
    setTimeout(() => {
      const idx = state.queues.notify.items.indexOf(orderId);
      if (idx !== -1) {
        state.queues.notify.items.splice(idx, 1);
        state.queues.notify.totalAcked++;
        flashConsumerAck(DOM.cNotifyNode, 'Notification Svc', orderId);
        renderQueues();
      }
    }, 600);

    // 3. Billing Service pulls IF NOT CRASHED
    if (!state.billingCrashed) {
      setTimeout(() => {
        const idx = state.queues.billing.items.indexOf(orderId);
        if (idx !== -1) {
          state.queues.billing.items.splice(idx, 1);
          state.queues.billing.totalAcked++;
          flashConsumerAck(DOM.cBillingNode, 'Billing Svc', orderId);
          renderQueues();
        }
      }, 750);
    } else {
      logFeed('BLOCKED_DRAIN', `<span style="color:#EF4444;">[STALLED]</span> Billing Service is crashed! Message <strong>${orderId}</strong> remains stuck in <code>billing.invoice.queue</code> buffer.`);
    }

    // 4. Fraud AI pulls if added
    if (state.fraudServiceAdded) {
      setTimeout(() => {
        const idx = state.queues.fraud.items.indexOf(orderId);
        if (idx !== -1) {
          state.queues.fraud.items.splice(idx, 1);
          state.queues.fraud.totalAcked++;
          flashConsumerAck(DOM.cFraudNode, 'ML Fraud AI', orderId);
          renderQueues();
        }
      }, 850);
    }
  }

  function flashConsumerAck(consumerNode, consumerName, msgId) {
    if (!consumerNode) return;
    consumerNode.classList.add('tmq-ack-flash');
    setTimeout(() => consumerNode.classList.remove('tmq-ack-flash'), 400);
    TMQAudio.purge();

    // Show destructive read badge
    const badge = document.createElement('div');
    badge.className = 'tmq-destructive-badge';
    badge.innerHTML = `💥 ACK &amp; DELETED: <code>${msgId}</code>`;
    consumerNode.appendChild(badge);
    setTimeout(() => badge.remove(), 1200);

    logFeed('DESTRUCTIVE_READ', `Consumer <strong>${consumerName}</strong> ACKed <code>${msgId}</code> &rarr; <span style="color:#FCA5A5; font-weight:700;">PURGED from broker memory! (Zero Replayability)</span>`);
  }

  function logFeed(type, htmlContent) {
    if (!DOM.feedList) return;
    const li = document.createElement('li');
    li.className = `tmq-log-item tmq-log-${type.toLowerCase()}`;
    const time = new Date().toLocaleTimeString();
    li.innerHTML = `<span class="tmq-log-time">${time}</span> <span class="tmq-log-type">[${type}]</span> <span class="tmq-log-body">${htmlContent}</span>`;
    DOM.feedList.insertBefore(li, DOM.feedList.firstChild);

    if (DOM.feedList.children.length > 25) {
      DOM.feedList.removeChild(DOM.feedList.lastChild);
    }
  }

  // --- Toggle Billing Crash Simulation ---
  function toggleBillingCrash() {
    TMQAudio.init();
    state.billingCrashed = !state.billingCrashed;

    if (state.billingCrashed) {
      DOM.btnCrash.innerHTML = `<span>💚</span> Recover Billing Service`;
      DOM.btnCrash.classList.add('btn-recovering');
      TMQAudio.alarm();
      logFeed('CRASH', `<strong style="color:#EF4444;">💥 CRASH SIMULATED:</strong> Billing & Invoicing Service has crashed! It has stopped acknowledging messages from <code>billing.invoice.queue</code>.`);
      
      // Auto-dispatch 3 fast messages to quickly demonstrate queue bloat and backpressure
      let counter = 0;
      const pushTimer = setInterval(() => {
        if (!state.billingCrashed || counter >= 4) {
          clearInterval(pushTimer);
          return;
        }
        dispatchMessage();
        counter++;
      }, 700);

    } else {
      DOM.btnCrash.innerHTML = `<span>💥</span> Crash/Slow Billing Service`;
      DOM.btnCrash.classList.remove('btn-recovering');
      hideAlert();
      TMQAudio.success();
      logFeed('RECOVERED', `<strong style="color:#10B981;">💚 RECOVERY:</strong> Billing Service restarted! Draining accumulated queue buffer...`);

      // Drain billing queue
      const drainInterval = setInterval(() => {
        if (state.queues.billing.items.length > 0) {
          const item = state.queues.billing.items.shift();
          flashConsumerAck(DOM.cBillingNode, 'Billing Svc', item);
          renderQueues();
        } else {
          clearInterval(drainInterval);
          renderQueues();
        }
      }, 300);
    }

    renderQueues();
  }

  // --- Add 4th Consumer (Fraud AI) ---
  function toggleAddConsumer() {
    TMQAudio.init();
    state.fraudServiceAdded = !state.fraudServiceAdded;

    if (state.fraudServiceAdded) {
      DOM.btnAddConsumer.innerHTML = `<span>➖</span> Remove Fraud AI Service`;
      DOM.btnAddConsumer.classList.add('btn-added');
      TMQAudio.success();
      showAlert(
        '⚠️ Architectural Burden: Adding Downstream Consumers in Traditional MQ',
        'In RabbitMQ, you CANNOT have Fraud AI read from orders.process.queue because doing so steals messages from Order Processing! You had to create a brand new queue (fraud.detection.queue), configure exchange routing keys, and duplicate message delivery storage across the broker!'
      );
      logFeed('NEW_SERVICE', `Added <strong>ML Fraud Detection AI</strong>. Created duplicate queue <code>fraud.detection.queue</code> with separate exchange binding.`);
    } else {
      DOM.btnAddConsumer.innerHTML = `<span>➕</span> Add 4th Consumer (Fraud AI)`;
      DOM.btnAddConsumer.classList.remove('btn-added');
      hideAlert();
      state.queues.fraud.items = [];
      logFeed('SERVICE_REMOVED', `Removed ML Fraud Detection AI and tore down <code>fraud.detection.queue</code>.`);
    }

    renderQueues();
  }

  // --- Attempt Replay Demonstration ---
  function showReplayError() {
    TMQAudio.init();
    TMQAudio.alarm();
    if (DOM.replayModal) {
      DOM.replayModal.classList.add('active');
    }
  }

  function hideReplayModal() {
    if (DOM.replayModal) {
      DOM.replayModal.classList.remove('active');
    }
  }

  // --- Reset Traditional MQ Simulation ---
  function resetSimulation() {
    state.billingCrashed = false;
    state.fraudServiceAdded = false;
    state.backpressureActive = false;
    state.memoryPercent = 20;

    state.queues.orders.items = ['ORD-101', 'ORD-102'];
    state.queues.billing.items = ['ORD-102'];
    state.queues.notify.items = ['ORD-102'];
    state.queues.fraud.items = [];

    DOM.btnCrash.innerHTML = `<span>💥</span> Crash/Slow Billing Service`;
    DOM.btnCrash.classList.remove('btn-recovering');
    DOM.btnAddConsumer.innerHTML = `<span>➕</span> Add 4th Consumer (Fraud AI)`;
    DOM.btnAddConsumer.classList.remove('btn-added');

    if (state.isAutoRunning) {
      clearInterval(state.autoInterval);
      state.isAutoRunning = false;
      DOM.btnAuto.innerHTML = `<span>▶</span> Live Traffic: OFF`;
      DOM.btnAuto.classList.remove('btn-streaming');
    }

    hideAlert();
    renderQueues();
    if (DOM.feedList) DOM.feedList.innerHTML = '';
    logFeed('RESET', 'Traditional MQ simulation reset. Buffer healthy at 20% memory.');
    TMQAudio.success();
  }

  function toggleAutoStream() {
    if (state.isAutoRunning) {
      clearInterval(state.autoInterval);
      state.isAutoRunning = false;
      DOM.btnAuto.innerHTML = `<span>▶</span> Live Traffic: OFF`;
      DOM.btnAuto.classList.remove('btn-streaming');
    } else {
      state.isAutoRunning = true;
      DOM.btnAuto.innerHTML = `<span>⏸</span> Live Traffic: ACTIVE`;
      DOM.btnAuto.classList.add('btn-streaming');
      dispatchMessage();
      state.autoInterval = setInterval(() => {
        if (!state.backpressureActive) {
          dispatchMessage();
        }
      }, 1600);
    }
  }

  // --- Setup Listeners ---
  function initEventListeners() {
    if (DOM.btnSend) DOM.btnSend.addEventListener('click', dispatchMessage);
    if (DOM.btnCrash) DOM.btnCrash.addEventListener('click', toggleBillingCrash);
    if (DOM.btnAddConsumer) DOM.btnAddConsumer.addEventListener('click', toggleAddConsumer);
    if (DOM.btnReplay) DOM.btnReplay.addEventListener('click', showReplayError);
    if (DOM.btnReset) DOM.btnReset.addEventListener('click', resetSimulation);
    if (DOM.btnAuto) DOM.btnAuto.addEventListener('click', toggleAutoStream);

    if (DOM.btnCloseAlert) DOM.btnCloseAlert.addEventListener('click', hideAlert);
    if (DOM.btnCloseReplayModal) DOM.btnCloseReplayModal.addEventListener('click', hideReplayModal);

    // Close replay modal on outside click
    window.addEventListener('click', (e) => {
      if (DOM.replayModal && e.target === DOM.replayModal) {
        hideReplayModal();
      }
    });

    // Close replay modal with Escape key
    window.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && DOM.replayModal && DOM.replayModal.classList.contains('active')) {
        hideReplayModal();
      }
    });
  }

  // --- Bootstrap ---
  function init() {
    initTabNavigation();
    cacheDOMElements();
    initEventListeners();
    renderQueues();
    logFeed('SYSTEM', 'Traditional MQ (RabbitMQ) visualizer ready. Click <strong>⚡ Send Transaction</strong> or <strong>💥 Crash Billing Service</strong> to inspect behavior.');
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
