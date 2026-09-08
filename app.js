// KafkaVisualizer - Fully Dynamic Architecture Simulator with Clean Microservice Cards

(function() {
  'use strict';

  // --- Dynamic Model State ---
  const state = {
    audioEnabled: true,
    numProducers: 3,
    numBrokers: 1,
    numPartitions: 3,
    numGroups: 2,

    // Partition states: { [partitionId]: { offset: 0, logs: [], leaderBroker: 1 } }
    partitions: {},

    // Consumer Groups state
    groups: {},

    // Canvas particles
    particles: [],

    // Live continuous streaming
    isStreaming: true,
    streamTimer: null,
    accountCycleIndex: 0
  };

  // Sample Accounts & Actions Bank for realistic financial distribution
  const SAMPLE_ACCOUNTS = [
    { key: 'ACC-303', desc: 'NYC Checking Account' },    // hashes to P-0
    { key: 'ACC-202', desc: 'Savings & Investment' },   // hashes to P-1
    { key: 'ACC-101', desc: 'Commercial Payroll' },     // hashes to P-2
    { key: 'ACC-404', desc: 'Venture Treasury' },       // hashes to P-1
    { key: 'ACC-505', desc: 'Retail Debit Account' },    // hashes to P-2
    { key: 'ACC-606', desc: 'Offshore International' }  // hashes to P-0
  ];

  const SAMPLE_EVENTS = [
    { text: '$500.00 Deposit (Salary Credit)', fraud: false },
    { text: '$120.00 ATM Cash Withdrawal', fraud: false },
    { text: '$45.50 Merchant POS Card Swipe', fraud: false },
    { text: '$2,400.00 International Wire Transfer', fraud: false },
    { text: '$85.00 Grocery Store Contactless', fraud: false },
    { text: '$15,000.00 Crypto Exchange Transfer', fraud: true },
    { text: '$320.00 Utility Bill Auto-Debit', fraud: false },
    { text: '$1,100.00 Rent Direct Payment', fraud: false }
  ];

  // --- Audio Synthesizer (Web Audio API) ---
  const AudioFX = {
    ctx: null,
    init() {
      if (!this.ctx) {
        const AudioCtx = window.AudioContext || window.webkitAudioContext;
        if (AudioCtx) this.ctx = new AudioCtx();
      }
    },
    play(freq, type = 'sine', dur = 0.08, vol = 0.05) {
      if (!state.audioEnabled || !this.ctx) return;
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
      } catch (e) {}
    },
    produce() { this.play(540, 'triangle', 0.05, 0.04); },
    append() {
      this.play(880, 'sine', 0.06, 0.03);
      setTimeout(() => this.play(1174, 'sine', 0.08, 0.03), 50);
    },
    commit() { this.play(740, 'sine', 0.06, 0.03); }
  };

  // --- Producer Definitions Bank ---
  const PRODUCER_DEFS = [
    { id: 'prod-mobile', name: 'mobile-banking-api-gateway', icon: '📱', desc: 'REST / Instant UPI Ingest (:8080)' },
    { id: 'prod-atm', name: 'atm-switch-ingest-service', icon: '🏧', desc: 'ISO 8583 Terminal Handler (:9010)' },
    { id: 'prod-pos', name: 'merchant-pos-card-service', icon: '💳', desc: 'Visa/Mastercard Swipes (:8088)' },
    { id: 'prod-wire', name: 'swift-wire-transfer-gateway', icon: '🌍', desc: 'Fedwire / Cross-Border (:8443)' },
    { id: 'prod-web', name: 'online-portal-bff-service', icon: '💻', desc: 'Web Banking BFF (:3000)' }
  ];

  // --- Consumer Group Definitions & Real Microservices Bank ---
  const GROUP_DEFS = [
    {
      id: 'cg-ledger',
      name: 'core-banking-ledger-group',
      icon: '💰',
      role: 'Idempotent Double-Entry Settlement & Balances',
      services: [
        { name: 'ledger-settlement-worker-svc', port: ':8081', actionVerb: 'Settled balance update' },
        { name: 'account-reconciliation-svc', port: ':8082', actionVerb: 'Audited balance consistency' },
        { name: 'fee-and-tax-calculator-svc', port: ':8083', actionVerb: 'Computed interchange fee' }
      ]
    },
    {
      id: 'cg-fraud',
      name: 'fraud-and-risk-engine-group',
      icon: '🛡️',
      role: 'Real-Time ML Scoring & AML Sanctions Checks',
      services: [
        { name: 'ml-anomaly-scoring-svc', port: ':9091', actionVerb: 'Scored ML risk pattern' },
        { name: 'geovelocity-distance-svc', port: ':9092', actionVerb: 'Verified travel velocity' },
        { name: 'aml-sanctions-screener-svc', port: ':9093', actionVerb: 'Screened OFAC blacklist' }
      ]
    },
    {
      id: 'cg-notify',
      name: 'customer-notifications-group',
      icon: '📬',
      role: 'Multi-Channel Alert & Statement Dispatcher',
      services: [
        { name: 'sms-twilio-gateway-svc', port: ':7071', actionVerb: 'Sent SMS OTP alert' },
        { name: 'firebase-push-notifier-svc', port: ':7072', actionVerb: 'Pushed mobile in-app notification' },
        { name: 'email-statement-dispatcher-svc', port: ':7073', actionVerb: 'Emailed PDF transaction receipt' }
      ]
    }
  ];

  // --- Partition Key Hasher ---
  function getPartitionForKey(key, numParts) {
    if (!key || numParts <= 0) return 0;
    let hash = 0;
    for (let i = 0; i < key.length; i++) {
      hash = ((hash << 5) - hash) + key.charCodeAt(i);
      hash |= 0;
    }
    return Math.abs(hash) % numParts;
  }

  // --- DOM Elements Cache ---
  const DOM = {
    canvas: document.getElementById('flow-canvas'),
    flowStatusText: document.getElementById('flow-status-text'),
    audioToggle: document.getElementById('audio-toggle'),
    audioStatus: document.getElementById('audio-status'),
    audioIcon: document.getElementById('audio-icon'),

    // Stepper Counters
    countProducers: document.getElementById('count-producers'),
    countBrokers: document.getElementById('count-brokers'),
    countPartitions: document.getElementById('count-partitions'),
    countGroups: document.getElementById('count-groups'),

    valProducers: document.getElementById('val-producers'),
    valBrokers: document.getElementById('val-brokers'),
    valPartitions: document.getElementById('val-partitions'),
    valGroups: document.getElementById('val-groups'),

    decProducers: document.getElementById('dec-producers'),
    incProducers: document.getElementById('inc-producers'),
    decBrokers: document.getElementById('dec-brokers'),
    incBrokers: document.getElementById('inc-brokers'),
    decPartitions: document.getElementById('dec-partitions'),
    incPartitions: document.getElementById('inc-partitions'),
    decGroups: document.getElementById('dec-groups'),
    incGroups: document.getElementById('inc-groups'),

    // Event Dispatcher Controls
    selectProducer: document.getElementById('select-producer-source'),
    inputAccountKey: document.getElementById('input-account-key'),
    targetPartText: document.getElementById('target-part-text'),
    selectEventAction: document.getElementById('select-event-action'),
    btnFireEvent: document.getElementById('btn-fire-event'),
    btnAutoStream: document.getElementById('btn-auto-stream'),
    streamIcon: document.getElementById('stream-icon'),
    streamText: document.getElementById('stream-text'),
    btnStepByStep: document.getElementById('btn-step-by-step'),
    btnResetAll: document.getElementById('btn-reset-all'),

    // Step Badges
    step1: document.getElementById('step-1-badge'),
    step2: document.getElementById('step-2-badge'),
    step3: document.getElementById('step-3-badge'),
    step4: document.getElementById('step-4-badge'),

    // Arena Containers
    producersContainer: document.getElementById('producers-container'),
    brokersContainer: document.getElementById('brokers-container'),
    partitionsContainer: document.getElementById('partitions-container'),
    groupsContainer: document.getElementById('groups-container'),

    // Audit Feed
    eventFeedTbody: document.getElementById('event-feed-tbody'),
    filterInput: document.getElementById('filter-input'),
    clearLogBtn: document.getElementById('clear-log-btn')
  };

  // --- Canvas Particle Animation ---
  let canvasCtx = null;

  function initCanvas() {
    if (!DOM.canvas) return;
    canvasCtx = DOM.canvas.getContext('2d');
    resizeCanvas();
    window.addEventListener('resize', resizeCanvas);
    requestAnimationFrame(renderCanvas);
  }

  function resizeCanvas() {
    if (!DOM.canvas) return;
    const rect = DOM.canvas.parentElement.getBoundingClientRect();
    DOM.canvas.width = rect.width;
    DOM.canvas.height = rect.height;
  }

  function getCenter(el) {
    if (!el || !DOM.canvas) return { x: 0, y: 0 };
    const cRect = DOM.canvas.getBoundingClientRect();
    const eRect = el.getBoundingClientRect();
    return {
      x: eRect.left + eRect.width / 2 - cRect.left,
      y: eRect.top + eRect.height / 2 - cRect.top
    };
  }

  function spawnParticle(fromEl, toEl, color, onDone) {
    if (!fromEl || !toEl) {
      if (onDone) onDone();
      return;
    }
    const start = getCenter(fromEl);
    const end = getCenter(toEl);
    const cpX = (start.x + end.x) / 2;
    const cpY = (start.y + end.y) / 2 - 20;

    state.particles.push({
      startX: start.x,
      startY: start.y,
      cpX: cpX,
      cpY: cpY,
      endX: end.x,
      endY: end.y,
      progress: 0,
      speed: 0.045,
      color: color || '#06B6D4',
      onDone: onDone
    });
  }

  function renderCanvas() {
    if (!canvasCtx || !DOM.canvas) return;
    canvasCtx.clearRect(0, 0, DOM.canvas.width, DOM.canvas.height);

    for (let i = state.particles.length - 1; i >= 0; i--) {
      const p = state.particles[i];
      p.progress += p.speed;

      if (p.progress >= 1) {
        if (p.onDone) p.onDone();
        state.particles.splice(i, 1);
        continue;
      }

      const t = p.progress;
      const invT = 1 - t;
      const x = invT * invT * p.startX + 2 * invT * t * p.cpX + t * t * p.endX;
      const y = invT * invT * p.startY + 2 * invT * t * p.cpY + t * t * p.endY;

      canvasCtx.save();
      canvasCtx.shadowColor = p.color;
      canvasCtx.shadowBlur = 10;
      canvasCtx.fillStyle = p.color;
      canvasCtx.beginPath();
      canvasCtx.arc(x, y, 4, 0, Math.PI * 2);
      canvasCtx.fill();
      canvasCtx.restore();
    }

    requestAnimationFrame(renderCanvas);
  }

  // --- Dynamic Model Initializers & Pre-Seeding ---

  function syncPartitionsState() {
    for (let p = 0; p < state.numPartitions; p++) {
      if (!state.partitions[p]) {
        state.partitions[p] = {
          offset: 0,
          logs: [],
          leaderBroker: 1
        };
      } else {
        state.partitions[p].leaderBroker = 1;
      }
    }
    Object.keys(state.partitions).forEach(p => {
      if (parseInt(p) >= state.numPartitions) delete state.partitions[p];
    });
  }

  function syncGroupsState(isReset = false) {
    for (let g = 0; g < state.numGroups; g++) {
      const def = GROUP_DEFS[g];
      const numConsumersInGroup = Math.min(state.numPartitions, 3);
      
      if (!state.groups[def.id]) {
        state.groups[def.id] = {
          name: def.name,
          icon: def.icon,
          role: def.role,
          consumers: []
        };
      }

      const currentConsumers = [];
      for (let c = 1; c <= numConsumersInGroup; c++) {
        const svcInfo = def.services[c - 1] || { name: `microservice-${c}`, port: `:808${c}`, actionVerb: 'Processed event' };
        const assignedParts = [];
        const offsets = {};

        for (let p = 0; p < state.numPartitions; p++) {
          if (p % numConsumersInGroup === c - 1) {
            assignedParts.push(p);
            if (isReset) {
              offsets[p] = 0;
            } else {
              const existingG = state.groups[def.id];
              const prevC = existingG && existingG.consumers ? existingG.consumers.find(con => con.id === c) : null;
              offsets[p] = (prevC && prevC.offsets[p] !== undefined) ? prevC.offsets[p] : 0;
            }
          }
        }

        const prevC = (!isReset && state.groups[def.id] && state.groups[def.id].consumers)
          ? state.groups[def.id].consumers.find(con => con.id === c) : null;
        const lastAct = (isReset || !prevC) ? `Waiting for events...` : prevC.lastActivity;

        currentConsumers.push({
          id: c,
          serviceName: svcInfo.name,
          port: svcInfo.port,
          actionVerb: svcInfo.actionVerb,
          assignedParts: assignedParts,
          offsets: offsets,
          lastActivity: lastAct
        });
      }

      state.groups[def.id].consumers = currentConsumers;
    }

    // Remove groups that exceed the current numGroups count
    const activeGroupIds = GROUP_DEFS.slice(0, state.numGroups).map(d => d.id);
    Object.keys(state.groups).forEach(gId => {
      if (!activeGroupIds.includes(gId)) delete state.groups[gId];
    });
  }

  // Pre-seed initial banking transactions across all partitions
  function preSeedInitialClusterData() {
    const seedData = [
      // Partition 0
      { key: 'ACC-303', action: '$120.00 ATM Cash Withdrawal', prod: 'atm-switch-ingest-service', fraud: false },
      { key: 'ACC-606', action: '$320.00 Utility Bill Auto-Debit', prod: 'online-portal-bff-service', fraud: false },
      { key: 'ACC-303', action: '$85.00 Grocery Contactless Pay', prod: 'merchant-pos-card-service', fraud: false },
      
      // Partition 1
      { key: 'ACC-202', action: '$500.00 Deposit (Salary Credit)', prod: 'mobile-banking-api-gateway', fraud: false },
      { key: 'ACC-404', action: '$1,100.00 Rent Direct Payment', prod: 'online-portal-bff-service', fraud: false },
      { key: 'ACC-202', action: '$45.50 Coffee & Lunch Swipe', prod: 'merchant-pos-card-service', fraud: false },
      
      // Partition 2
      { key: 'ACC-101', action: '$2,400.00 International Wire Transfer', prod: 'swift-wire-transfer-gateway', fraud: false },
      { key: 'ACC-505', action: '$150.00 Mobile Instant UPI', prod: 'mobile-banking-api-gateway', fraud: false },
      { key: 'ACC-101', action: '$15,000.00 Crypto Exchange Transfer', prod: 'mobile-banking-api-gateway', fraud: true }
    ];

    seedData.forEach(item => {
      const targetP = getPartitionForKey(item.key, state.numPartitions);
      const pData = state.partitions[targetP];
      if (pData) {
        const off = pData.offset;
        pData.offset++;
        const log = {
          offset: off,
          key: item.key,
          action: item.action,
          isFraud: item.fraud,
          time: new Date(Date.now() - (10 - off) * 60000).toLocaleTimeString(),
          producer: item.prod,
          partition: targetP,
          broker: 1
        };
        pData.logs.push(log);

        Object.keys(state.groups).forEach(gId => {
          const group = state.groups[gId];
          const con = group.consumers.find(c => c.assignedParts.includes(targetP));
          if (con) {
            con.offsets[targetP] = off + 1;
            con.lastActivity = `${con.actionVerb} on [${item.key}]`;
          }
        });
      }
    });
  }

  // --- UI Renderers for N-Tiers ---

  function renderProducers() {
    DOM.producersContainer.innerHTML = '';
    DOM.selectProducer.innerHTML = '';

    for (let i = 0; i < state.numProducers; i++) {
      const def = PRODUCER_DEFS[i];
      
      const card = document.createElement('div');
      card.className = 'producer-card';
      card.id = `node-${def.id}`;
      card.innerHTML = `
        <div class="prod-icon">${def.icon}</div>
        <div class="prod-info">
          <h4><code>${def.name}</code></h4>
          <span>${def.desc}</span>
        </div>
      `;
      card.addEventListener('click', () => {
        DOM.selectProducer.value = def.id;
        highlightActiveStep(1);
      });
      DOM.producersContainer.appendChild(card);

      const opt = document.createElement('option');
      opt.value = def.id;
      opt.textContent = `${def.icon} ${def.name}`;
      DOM.selectProducer.appendChild(opt);
    }
  }

  function renderBrokers() {
    DOM.brokersContainer.innerHTML = '';
    for (let b = 1; b <= state.numBrokers; b++) {
      const box = document.createElement('div');
      box.className = 'broker-box';
      box.id = `broker-node-${b}`;
      
      const partsList = [];
      for (let p = 0; p < state.numPartitions; p++) {
        partsList.push(`P-${p}`);
      }

      box.innerHTML = `
        <div style="display: flex; align-items: center; justify-content: space-between; width: 100%; margin-bottom: 2px;">
          <span class="broker-id" style="color: var(--color-cyan);">Broker 1 (ID: 101) &bull; localhost:9092</span>
          <span class="cluster-badge" style="background: rgba(16, 185, 129, 0.15); color: var(--color-emerald); border-color: rgba(16, 185, 129, 0.3);">Leader for all Partitions</span>
        </div>
        <span class="broker-role" style="font-size: 0.7rem; color: var(--text-secondary);">Hosting Topic: <code>bank-transactions</code> [Partitions: ${partsList.join(', ')}]</span>
      `;
      DOM.brokersContainer.appendChild(box);
    }
  }

  function renderPartitions() {
    DOM.partitionsContainer.innerHTML = '';
    
    for (let p = 0; p < state.numPartitions; p++) {
      const pData = state.partitions[p] || { offset: 0, logs: [], leaderBroker: 1 };
      const box = document.createElement('div');
      box.className = 'partition-box';
      box.id = `part-node-${p}`;
      
      box.innerHTML = `
        <div class="partition-header-row">
          <span class="part-badge-tag">Partition ${p} (Broker 1)</span>
          <div class="part-meta-info">
            <span class="offset-counter-tag">Offset: <strong id="p-offset-val-${p}">${pData.offset}</strong></span>
            <span class="hw-tag">HW: ${pData.offset}</span>
          </div>
        </div>
        <div class="log-tape" id="tape-part-${p}">
          ${pData.logs.length === 0 ? '<span class="empty-tape-msg">Log empty (Offsets start at #0)...</span>' : ''}
        </div>
      `;

      DOM.partitionsContainer.appendChild(box);

      const tape = box.querySelector(`#tape-part-${p}`);
      if (pData.logs.length > 0) {
        tape.innerHTML = '';
        pData.logs.slice(-8).forEach(log => {
          const block = createLogBlockElem(log);
          tape.appendChild(block);
        });
      }
    }
  }

  function renderGroups() {
    DOM.groupsContainer.innerHTML = '';
    
    for (let g = 0; g < state.numGroups; g++) {
      const def = GROUP_DEFS[g];
      const gData = state.groups[def.id] || { consumers: [] };
      
      const card = document.createElement('div');
      card.className = 'group-card';
      card.id = `group-node-${def.id}`;

      let consumersHtml = '';
      gData.consumers.forEach(c => {
        const assignedLabels = c.assignedParts.map(p => `P-${p}`).join(', ');
        
        const offsetLabels = c.assignedParts.map(p => {
          const off = c.offsets[p] !== undefined ? c.offsets[p] : 0;
          return `P-${p} Offset: #${off}`;
        }).join(' | ');

        consumersHtml += `
          <div class="consumer-item" id="consumer-item-${def.id}-${c.id}">
            <div class="consumer-top-line">
              <span class="c-id">
                <span class="c-dot"></span>
                <code>${c.serviceName}</code> <small style="color: var(--text-muted); font-size: 0.65rem;">${c.port}</small>
              </span>
              <div class="c-meta-badges">
                <span class="c-part-assigned">Assigned: ${assignedLabels}</span>
                <span class="c-offset-badge" id="c-badge-offset-${def.id}-${c.id}">${offsetLabels}</span>
              </div>
            </div>
            <div class="consumer-action-subtitle" id="c-act-${def.id}-${c.id}">
              <span>&bull; ${c.lastActivity}</span>
            </div>
          </div>
        `;
      });

      card.innerHTML = `
        <div class="group-header">
          <div class="group-title-row">
            <span class="group-icon">${def.icon}</span>
            <div>
              <h4><code>${def.name}</code></h4>
              <span class="group-id-code">${def.role}</span>
            </div>
          </div>
          <span class="cg-commit-badge">Group Active</span>
        </div>
        <div class="consumers-list">
          ${consumersHtml}
        </div>
      `;

      DOM.groupsContainer.appendChild(card);
    }
  }

  function createLogBlockElem(log) {
    const block = document.createElement('div');
    block.className = 'log-block' + (log.isFraud ? ' fraud-block' : '');
    block.innerHTML = `<strong>#${log.offset}</strong> <span>${log.key} (${log.action})</span>`;
    return block;
  }

  function refreshAllTiers() {
    syncPartitionsState();
    syncGroupsState();
    renderProducers();
    renderBrokers();
    renderPartitions();
    renderGroups();
    updateHashPreview();
  }

  function updateHashPreview() {
    const key = DOM.inputAccountKey.value.trim() || 'ACC-101';
    const targetP = getPartitionForKey(key, state.numPartitions);
    DOM.targetPartText.textContent = `P-${targetP}`;
    DOM.hashCalcBadge = document.getElementById('hash-calc-badge');
    if (DOM.hashCalcBadge) {
      DOM.hashCalcBadge.innerHTML = `<code>hash("${key}") % ${state.numPartitions}</code> = <strong>P-${targetP}</strong>`;
    }
  }

  function highlightActiveStep(stepNum) {
    [DOM.step1, DOM.step2, DOM.step3, DOM.step4].forEach((el, idx) => {
      if (el) el.classList.toggle('active-step', idx + 1 === stepNum);
    });
  }

  // --- Event Dispatch Pipeline (The Core Kafka Flow) ---
  function dispatchEventFlow(customKey, customAction, isStepMode = false) {
    AudioFX.init();
    
    const key = customKey || DOM.inputAccountKey.value.trim() || 'ACC-101';
    const prodId = DOM.selectProducer.value || PRODUCER_DEFS[0].id;
    const prodDef = PRODUCER_DEFS.find(p => p.id === prodId) || PRODUCER_DEFS[0];
    const action = customAction || DOM.selectEventAction.options[DOM.selectEventAction.selectedIndex].text;
    const isFraud = action.includes('Suspicious') || action.includes('Crypto');

    // 1. Compute Partition
    const targetP = getPartitionForKey(key, state.numPartitions);
    const pData = state.partitions[targetP];

    // UI Step 1: Producer Key Hashing
    highlightActiveStep(1);
    DOM.flowStatusText.textContent = `Step 1: ${prodDef.name} computed hash("${key}") % ${state.numPartitions} -> Partition P-${targetP}`;
    AudioFX.produce();

    const prodElem = document.getElementById(`node-${prodId}`);
    if (prodElem) {
      prodElem.classList.add('active-flash');
      setTimeout(() => prodElem.classList.remove('active-flash'), 300);
    }

    // UI Step 2: Ingestion by Broker
    const brokerElem = document.getElementById(`broker-node-1`);
    const partElem = document.getElementById(`part-node-${targetP}`);

    spawnParticle(prodElem, brokerElem || partElem, isFraud ? '#F59E0B' : '#06B6D4', () => {
      highlightActiveStep(2);
      DOM.flowStatusText.textContent = `Step 2: Broker 1 received record for Partition ${targetP} on Topic 'bank-transactions'`;
      if (brokerElem) {
        brokerElem.classList.add('active-broker');
        setTimeout(() => brokerElem.classList.remove('active-broker'), 300);
      }

      // UI Step 3: Write-Ahead Log (Offset Append)
      setTimeout(() => {
        highlightActiveStep(3);
        const assignedOffset = pData.offset;
        pData.offset++;
        
        const logRecord = {
          offset: assignedOffset,
          key: key,
          action: action,
          isFraud: isFraud,
          time: new Date().toLocaleTimeString(),
          producer: prodDef.name,
          partition: targetP,
          broker: 1
        };
        pData.logs.push(logRecord);

        // Update offset display on Partition Box
        const offsetValEl = document.getElementById(`p-offset-val-${targetP}`);
        if (offsetValEl) offsetValEl.textContent = pData.offset;

        // Append to visual log tape
        const tape = document.getElementById(`tape-part-${targetP}`);
        if (tape) {
          const emptyMsg = tape.querySelector('.empty-tape-msg');
          if (emptyMsg) emptyMsg.remove();

          const block = createLogBlockElem(logRecord);
          tape.appendChild(block);
          tape.scrollLeft = tape.scrollWidth;
        }

        if (partElem) {
          partElem.classList.add('active-append');
          setTimeout(() => partElem.classList.remove('active-append'), 300);
        }

        AudioFX.append();
        DOM.flowStatusText.textContent = `Step 3: Appended to Partition P-${targetP} at Log Offset #${assignedOffset}`;

        // UI Step 4: Fan-out to Microservices in Consumer Groups
        setTimeout(() => {
          highlightActiveStep(4);
          DOM.flowStatusText.textContent = `Step 4: Assigned Microservices consumed Partition P-${targetP} and committed offsets`;

          Object.keys(state.groups).forEach(gId => {
            const groupObj = state.groups[gId];
            
            const activeConsumer = groupObj.consumers.find(c => c.assignedParts.includes(targetP));
            
            if (activeConsumer) {
              const cItemEl = document.getElementById(`consumer-item-${gId}-${activeConsumer.id}`);
              
              spawnParticle(partElem, cItemEl || document.getElementById(`group-node-${gId}`), '#10B981', () => {
                activeConsumer.offsets[targetP] = assignedOffset + 1;
                activeConsumer.lastActivity = `${activeConsumer.actionVerb} on [${key}]`;

                if (cItemEl) {
                  cItemEl.classList.add('active-consuming');
                  setTimeout(() => cItemEl.classList.remove('active-consuming'), 400);

                  const badgeEl = document.getElementById(`c-badge-offset-${gId}-${activeConsumer.id}`);
                  if (badgeEl) {
                    badgeEl.textContent = activeConsumer.assignedParts.map(p => {
                      const off = activeConsumer.offsets[p] !== undefined ? activeConsumer.offsets[p] : 0;
                      return `P-${p} Offset: #${off}`;
                    }).join(' | ');
                  }

                  const actEl = document.getElementById(`c-act-${gId}-${activeConsumer.id}`);
                  if (actEl) {
                    actEl.innerHTML = `<span>&bull; ${activeConsumer.actionVerb} on <strong>[${key}]</strong> &rarr; Commit: <strong>#${assignedOffset + 1}</strong></span>`;
                  }
                }

                AudioFX.commit();
              });
            }
          });

          // Add to Audit Table
          addAuditRow(logRecord);
        }, isStepMode ? 800 : 350);

      }, isStepMode ? 700 : 250);
    });
  }

  function addAuditRow(log) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${log.time}</td>
      <td><code>${log.producer}</code></td>
      <td><span style="color: #06B6D4; font-weight: 700;">${log.key}</span></td>
      <td><code>hash("${log.key}") % ${state.numPartitions}</code></td>
      <td><span class="tag-part">Partition ${log.partition}</span></td>
      <td><strong style="color: #10B981;">Offset #${log.offset}</strong></td>
      <td>${log.action}</td>
      <td><span class="tag-commit">Committed #${log.offset + 1}</span></td>
      <td><span class="tag-commit">Committed #${log.offset + 1}</span></td>
    `;
    DOM.eventFeedTbody.insertBefore(tr, DOM.eventFeedTbody.firstChild);

    if (DOM.eventFeedTbody.children.length > 30) {
      DOM.eventFeedTbody.removeChild(DOM.eventFeedTbody.lastChild);
    }
  }

  function populateInitialAuditTable() {
    DOM.eventFeedTbody.innerHTML = '';
    for (let p = 0; p < state.numPartitions; p++) {
      const pData = state.partitions[p];
      if (pData && pData.logs) {
        pData.logs.forEach(log => addAuditRow(log));
      }
    }
  }

  // --- Controls & Steppers Listeners ---
  function initControls() {
    // Producers Stepper
    DOM.incProducers.addEventListener('click', () => {
      if (state.numProducers < 5) {
        state.numProducers++;
        DOM.countProducers.textContent = state.numProducers;
        DOM.valProducers.textContent = `${state.numProducers} Producers`;
        renderProducers();
      }
    });
    DOM.decProducers.addEventListener('click', () => {
      if (state.numProducers > 1) {
        state.numProducers--;
        DOM.countProducers.textContent = state.numProducers;
        DOM.valProducers.textContent = `${state.numProducers} Producers`;
        renderProducers();
      }
    });

    // Brokers Stepper
    DOM.incBrokers.addEventListener('click', () => {
      if (state.numBrokers < 4) {
        state.numBrokers++;
        DOM.countBrokers.textContent = state.numBrokers;
        DOM.valBrokers.textContent = `${state.numBrokers} Brokers`;
        refreshAllTiers();
      }
    });
    DOM.decBrokers.addEventListener('click', () => {
      if (state.numBrokers > 1) {
        state.numBrokers--;
        DOM.countBrokers.textContent = state.numBrokers;
        DOM.valBrokers.textContent = `${state.numBrokers} Brokers`;
        refreshAllTiers();
      }
    });

    // Partitions Stepper
    DOM.incPartitions.addEventListener('click', () => {
      if (state.numPartitions < 6) {
        state.numPartitions++;
        DOM.countPartitions.textContent = state.numPartitions;
        DOM.valPartitions.textContent = `${state.numPartitions} Partitions`;
        refreshAllTiers();
      }
    });
    DOM.decPartitions.addEventListener('click', () => {
      if (state.numPartitions > 1) {
        state.numPartitions--;
        DOM.countPartitions.textContent = state.numPartitions;
        DOM.valPartitions.textContent = `${state.numPartitions} Partitions`;
        refreshAllTiers();
      }
    });

    // Consumer Groups Stepper
    DOM.incGroups.addEventListener('click', () => {
      if (state.numGroups < 3) {
        state.numGroups++;
        DOM.countGroups.textContent = state.numGroups;
        DOM.valGroups.textContent = `${state.numGroups} Groups`;
        syncGroupsState();
        renderGroups();
      }
    });
    DOM.decGroups.addEventListener('click', () => {
      if (state.numGroups > 1) {
        state.numGroups--;
        DOM.countGroups.textContent = state.numGroups;
        DOM.valGroups.textContent = `${state.numGroups} Groups`;
        syncGroupsState();
        renderGroups();
      }
    });

    // Account key typing
    DOM.inputAccountKey.addEventListener('input', updateHashPreview);

    // Send Event Button
    DOM.btnFireEvent.addEventListener('click', () => {
      const currentAccount = SAMPLE_ACCOUNTS[state.accountCycleIndex % SAMPLE_ACCOUNTS.length];
      state.accountCycleIndex++;
      DOM.inputAccountKey.value = currentAccount.key;
      updateHashPreview();

      const randomProdIdx = Math.floor(Math.random() * state.numProducers);
      DOM.selectProducer.selectedIndex = randomProdIdx;

      dispatchEventFlow(currentAccount.key, null, false);
    });

    // Step-by-Step Mode Button
    DOM.btnStepByStep.addEventListener('click', () => {
      const currentAccount = SAMPLE_ACCOUNTS[state.accountCycleIndex % SAMPLE_ACCOUNTS.length];
      state.accountCycleIndex++;
      DOM.inputAccountKey.value = currentAccount.key;
      updateHashPreview();
      dispatchEventFlow(currentAccount.key, null, true);
    });

    // Auto-Stream (Continuous Live Traffic)
    DOM.btnAutoStream.addEventListener('click', toggleAutoStream);

    // Reset All State — clears everything, no pre-seeding
    DOM.btnResetAll.addEventListener('click', () => {
      if (state.isStreaming) {
        clearInterval(state.streamTimer);
        state.isStreaming = false;
        DOM.streamIcon.textContent = '▶';
        DOM.streamText.textContent = 'Live Traffic: OFF';
        DOM.btnAutoStream.classList.remove('btn-streaming');
      }
      state.partitions = {};
      state.groups = {};
      state.particles = [];
      state.accountCycleIndex = 0;
      DOM.eventFeedTbody.innerHTML = '';
      syncPartitionsState();        // creates fresh partitions with offset 0
      syncGroupsState(true);        // creates fresh consumers with offset 0 & 'Waiting for events...'
      renderProducers();
      renderBrokers();
      renderPartitions();
      renderGroups();
      updateHashPreview();
      DOM.flowStatusText.textContent = 'Cluster reset. All offsets cleared. No data — ready for fresh events!';
    });

    // Audio Toggle
    DOM.audioToggle.addEventListener('click', () => {
      state.audioEnabled = !state.audioEnabled;
      DOM.audioStatus.textContent = state.audioEnabled ? 'ON' : 'OFF';
      DOM.audioIcon.textContent = state.audioEnabled ? '🔊' : '🔇';
    });

    // Filter Feed
    DOM.filterInput.addEventListener('input', () => {
      const q = DOM.filterInput.value.toLowerCase().trim();
      const rows = DOM.eventFeedTbody.querySelectorAll('tr');
      rows.forEach(r => {
        r.style.display = r.textContent.toLowerCase().includes(q) ? '' : 'none';
      });
    });

    DOM.clearLogBtn.addEventListener('click', () => {
      DOM.eventFeedTbody.innerHTML = '';
    });
  }

  function toggleAutoStream() {
    if (state.isStreaming) {
      clearInterval(state.streamTimer);
      state.isStreaming = false;
      DOM.streamIcon.textContent = '▶';
      DOM.streamText.textContent = 'Live Traffic: OFF';
      return;
    }

    startAutoStream();
  }

  function startAutoStream() {
    state.isStreaming = true;
    DOM.streamIcon.textContent = '⏸';
    DOM.streamText.textContent = 'Live Traffic: ACTIVE';

    state.streamTimer = setInterval(() => {
      const rndAccount = SAMPLE_ACCOUNTS[Math.floor(Math.random() * SAMPLE_ACCOUNTS.length)];
      const rndEvent = SAMPLE_EVENTS[Math.floor(Math.random() * SAMPLE_EVENTS.length)];
      const randomProdIdx = Math.floor(Math.random() * state.numProducers);
      
      DOM.selectProducer.selectedIndex = randomProdIdx;
      DOM.inputAccountKey.value = rndAccount.key;
      updateHashPreview();

      dispatchEventFlow(rndAccount.key, rndEvent.text, false);
    }, 1200);
  }

  // --- App Bootstrap ---
  function init() {
    initCanvas();
    syncPartitionsState();
    syncGroupsState();
    preSeedInitialClusterData();
    renderProducers();
    renderBrokers();
    renderPartitions();
    renderGroups();
    populateInitialAuditTable();
    initControls();
    updateHashPreview();
    // Auto-stream does NOT start automatically — user clicks Live Traffic button to activate
    state.isStreaming = false;
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
