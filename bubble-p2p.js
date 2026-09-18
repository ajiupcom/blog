/* ============ bubble-p2p.js ============
   BubbleP2P — 通用的 WebRTC 点对点直连管理类，从原 index.html 里的
   P2P IIFE 提炼出来，逻辑完全一致（ACK+去重+心跳、perfect negotiation、
   连接体检/超时回收），唯一的区别是：不再读任何全局变量，所有外部依赖
   都通过构造函数注入，因此可以脱离这个具体项目被复用/测试。

   用法：
     const p2p = new BubbleP2P({
       myUserId: state.me.userId,
       // 把一条信令（offer/answer/ice/自定义信令）加密后经由你自己的
       // 中转通道发给对方；失败/查不到公钥就什么都不做（P2P 会自动
       // 放弃这轮直连，交给上层的兜底通道）
       sendSignalEnvelope: async (peerUid, signal) => {
         const envelope = await encryptForPeer(peerUid, signal);
         if (!envelope) return;
         BubbleComm.send(peerUid, { kind: 'rtc_sig_env', from: state.me.userId, envelope });
       },
       getIceServers: async () => { ... },
       getConnectionMode: () => state.connectionModeUi, // 'p2p' | 'relay'
       onBadgeChange: (peerUid) => updateP2PBadge(peerUid), // 可选，UI状态点
     });

     p2p.setRemoteTrackHandler((peerUid, ev) => Call.handleRemoteTrack(peerUid, ev));
     p2p.setCustomSignalHandler((peerUid, signal) => Call.handleCallSignal(peerUid, signal));
     p2p.setChannelOpenHandler((peerUid) => resendUndeliveredTo(peerUid));
     p2p.setPeerClosedHandler((peerUid) => Call.onPeerClosed(peerUid));

     // 收到经中转通道转发来的信令后：
     p2p.handleSignal(fromUid, decryptedSignal);
*/
class BubbleP2P {
  /**
   * @param {object} opts
   * @param {string} opts.myUserId
   * @param {(peerUid: string, signal: object) => Promise<void>} opts.sendSignalEnvelope
   *        把一条信令发给对方（已经处理好加密/中转），失败时静默即可。
   * @param {() => Promise<Array<RTCIceServer>>} opts.getIceServers
   * @param {() => ('p2p'|'relay')} [opts.getConnectionMode] - 不传则始终按 'p2p'（直连优先）处理
   * @param {(peerUid: string) => void} [opts.onBadgeChange] - 连接状态变化时的可选回调（更新UI用）
   * @param {object} [opts.timeouts] - 各类超时参数，不传则用默认值（与原实现一致）
   */
  constructor(opts) {
    if (!opts || !opts.myUserId) throw new Error('BubbleP2P: 需要 myUserId');
    if (typeof opts.sendSignalEnvelope !== 'function') throw new Error('BubbleP2P: 需要 sendSignalEnvelope(peerUid, signal)');
    if (typeof opts.getIceServers !== 'function') throw new Error('BubbleP2P: 需要 getIceServers()');

    this.myUserId = opts.myUserId;
    this._sendSignalEnvelope = opts.sendSignalEnvelope;
    this._getIceServers = opts.getIceServers;
    this._getConnectionMode = opts.getConnectionMode || (() => 'p2p');
    this._onBadgeChange = opts.onBadgeChange || (() => {});

    const t = opts.timeouts || {};
    // 直连打不通、走TURN中继的连接，比纯直连多一跳（自己<->TURN服务器<->
    // 对方），延迟必然更高。下面这几个超时原来是按纯直连的网络状况调的，
    // 配上TURN之后同样的数字会显得过于激进——ACK等不到就直接把整条连接
    // 推倒重来，连接卡在"connecting"稍微久一点也直接判定失败重连——网络
    // 稍微抖一下就触发一次不必要的重置，表现出来就是"P2P一会连上一会断"。
    // 默认值统一放宽一些；仍然可以通过构造函数覆盖。
    this.ACK_TIMEOUT_MS = t.ackTimeoutMs ?? 5000;
    this.HEARTBEAT_IDLE_MS = t.heartbeatIdleMs ?? 35 * 1000;
    this.HEARTBEAT_TIMEOUT_MS = t.heartbeatTimeoutMs ?? 5000;
    this.RECENT_MSGID_CAP = t.recentMsgIdCap ?? 500;
    this.IDLE_TIMEOUT_MS = t.idleTimeoutMs ?? 5 * 60 * 1000;
    this.HARD_MAX_MS = t.hardMaxMs ?? 15 * 60 * 1000;
    this.STUCK_TIMEOUT_MS = t.stuckTimeoutMs ?? 20 * 1000;
    this.DISCONNECT_GRACE_MS = t.disconnectGraceMs ?? 4000; // ICE 'disconnected' 的宽限窗口，见 oniceconnectionstatechange

    // 单条数据通道消息的分片大小、以及触发背压等待的发送缓冲区高水位——
    // 见 _sendRaw/_sendWithBackpressure。图片/文件这类内容常常是几百KB
    // 到几MB的base64文本，远超WebRTC数据通道单条消息建议的安全上限
    // （不同浏览器/版本协商到的max-message-size不一致，貌似能用不代表
    // 到处都能用），一次性 channel.send() 一个大字符串经常直接抛
    // OperationError: Failure to send data。拆成小片分开发送，既避开
    // 单条消息大小限制，配合背压等待也不会把发送缓冲区一口气灌爆。
    this.CHUNK_SIZE = t.chunkSizeBytes ?? 16 * 1024;
    this.CHUNK_HIGH_WATERMARK = t.chunkHighWatermarkBytes ?? 256 * 1024;

    this.peers = {};        // peerUid -> {pc, channel, polite, makingOffer, ignoreOffer, channelCreated, createdAt, lastActivityTs, heartbeatMissed, heartbeatTimer}
    this._pendingPeers = {};
    this._pendingAcks = new Map();
    this._pendingPings = new Map();
    this._recentMsgIds = new Map();

    this._onCustomSignal = null;
    this._onRemoteTrack = null;
    this._onChannelOpen = null;
    this._onPeerClosed = null;
    this._onIncomingPayload = null; // 业务消息回调：(peerUid, payload) => void
  }

  // ---------- 外部挂钩 ----------
  setCustomSignalHandler(fn) { this._onCustomSignal = fn; }
  setRemoteTrackHandler(fn) { this._onRemoteTrack = fn; }
  setChannelOpenHandler(fn) { this._onChannelOpen = fn; }
  setPeerClosedHandler(fn) { this._onPeerClosed = fn; }
  /** 收到经数据通道送达的业务消息（已去重/已回ACK）时触发 */
  setIncomingPayloadHandler(fn) { this._onIncomingPayload = fn; }

  _isPolite(peerUid) { return this.myUserId < peerUid; }

  _rememberMsgId(msgId) {
    this._recentMsgIds.set(msgId, Date.now());
    if (this._recentMsgIds.size > this.RECENT_MSGID_CAP) {
      const oldest = this._recentMsgIds.keys().next().value;
      this._recentMsgIds.delete(oldest);
    }
  }

  async sendSignal(peerUid, signal) {
    await this._sendSignalEnvelope(peerUid, signal);
  }

  async ensurePeer(peerUid) {
    if (this.peers[peerUid]) return this.peers[peerUid];
    if (this._pendingPeers[peerUid]) return this._pendingPeers[peerUid];

    const creating = (async () => {
      if (typeof RTCPeerConnection === 'undefined') {
        console.warn('[BubbleP2P] 当前环境不支持 WebRTC');
        return null;
      }
      const iceServers = await this._getIceServers();
      const hasTurn = iceServers.length > 1;
      const iceTransportPolicy = (this._getConnectionMode() === 'relay' && hasTurn) ? 'relay' : 'all';
      const pc = new RTCPeerConnection({ iceServers, iceTransportPolicy });
      const entry = {
        pc, channel: null, polite: this._isPolite(peerUid), makingOffer: false,
        ignoreOffer: false, channelCreated: false, createdAt: Date.now(),
        lastActivityTs: Date.now(), heartbeatMissed: 0, heartbeatTimer: null,
        disconnectGraceTimer: null,
        pendingCandidates: [], // 远端 SDP 还没 setRemoteDescription 完成时，先到的 ICE candidate 缓在这里
      };
      this.peers[peerUid] = entry;

      pc.onicecandidate = (ev) => { if (ev.candidate) this.sendSignal(peerUid, { t: 'ice', candidate: ev.candidate }); };

      pc.onnegotiationneeded = async () => {
        try {
          entry.makingOffer = true;
          await pc.setLocalDescription();
          this.sendSignal(peerUid, { t: 'sdp', description: pc.localDescription });
        } catch (e) { console.warn('[BubbleP2P] 协商失败', e); }
        finally { entry.makingOffer = false; }
      };

      pc.ondatachannel = (ev) => this._attachChannel(peerUid, ev.channel);
      pc.ontrack = (ev) => this._onRemoteTrack?.(peerUid, ev);

      // 'disconnected' 常常只是几秒钟的网络抖动（切换 WiFi/4G、系统短暂
      // 休眠网络栈），几秒内会自己恢复；一收到就立刻销毁连接，反而会把
      // 本来能自愈的连接（以及正在进行的通话）打断。'failed'/'closed' 才
      // 是真正确定没救了，立刻清理。'disconnected' 给一个宽限窗口，
      // 窗口内状态恢复了就什么都不做，超时还没恢复才真正清理重建。
      pc.oniceconnectionstatechange = () => {
        if (pc.iceConnectionState === 'disconnected') {
          if (entry.disconnectGraceTimer) return; // 已经在等了，不重复设置
          entry.disconnectGraceTimer = setTimeout(() => {
            entry.disconnectGraceTimer = null;
            if (this.peers[peerUid] && pc.iceConnectionState === 'disconnected') {
              this.resetPeerConnection(peerUid, true);
            }
          }, this.DISCONNECT_GRACE_MS);
        } else if (entry.disconnectGraceTimer && (pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed')) {
          clearTimeout(entry.disconnectGraceTimer);
          entry.disconnectGraceTimer = null;
        }
      };

      pc.onconnectionstatechange = () => {
        this._onBadgeChange(peerUid);
        if (['failed', 'closed'].includes(pc.connectionState)) this.cleanupPeer(peerUid);
      };

      this._startHeartbeat(peerUid);
      return entry;
    })();

    this._pendingPeers[peerUid] = creating;
    const result = await creating;
    delete this._pendingPeers[peerUid];
    return result;
  }

  _attachChannel(peerUid, channel) {
    const entry = this.peers[peerUid];
    if (!entry) return;
    entry.channel = channel;
    entry.incomingXfers = {}; // xferId -> {parts, got, total} —— 见 onmessage 里 p2p-chunk-* 的处理

    channel.onopen = () => { this._onBadgeChange(peerUid); this._onChannelOpen?.(peerUid); };
    channel.onclose = () => { if (entry.channel === channel) entry.channel = null; this._onBadgeChange(peerUid); };
    channel.onerror = (e) => console.warn('[BubbleP2P] 数据通道出错', e);
    channel.onmessage = (ev) => {
      entry.lastActivityTs = Date.now();
      let frame;
      try { frame = JSON.parse(ev.data); } catch (e) { return; }

      // 大消息被 _sendRaw 拆成了多片发过来（见该方法的说明）——这三种
      // kind 只是"重新拼出原始payload"这一步的框架消息，本身不是业务
      // 消息，拼完整之后才当成一条正常收到的payload走下面 _handlePayload。
      if (frame.kind === 'p2p-chunk-begin') {
        entry.incomingXfers[frame.xferId] = { parts: new Array(frame.total), got: 0, total: frame.total };
        return;
      }
      if (frame.kind === 'p2p-chunk') {
        const xfer = entry.incomingXfers[frame.xferId];
        if (!xfer) return; // 没收到begin（比如中途重连了，之前没拼完的直接作废）——丢这一片，重传交给上层"没收到ack就重发"兜底
        if (xfer.parts[frame.seq] === undefined) xfer.got++;
        xfer.parts[frame.seq] = frame.data;
        return;
      }
      if (frame.kind === 'p2p-chunk-end') {
        const xfer = entry.incomingXfers[frame.xferId];
        delete entry.incomingXfers[frame.xferId];
        if (!xfer || xfer.got !== xfer.total) { console.warn('[BubbleP2P] 分片消息不完整，丢弃', frame.xferId); return; }
        let payload;
        try { payload = JSON.parse(xfer.parts.join('')); } catch (e) { return; }
        this._handlePayload(peerUid, channel, payload);
        return;
      }

      this._handlePayload(peerUid, channel, frame);
    };
  }

  /** 处理一条"已经拼完整"的业务/控制消息——不管它本来是一条小消息直接
      收到的，还是拆片收到后拼出来的，从这里开始逻辑完全一样。 */
  _handlePayload(peerUid, channel, payload) {
    if (payload.kind === 'p2p-ack') {
      const resolve = this._pendingAcks.get(payload.msgId);
      if (resolve) { this._pendingAcks.delete(payload.msgId); resolve(); }
      return;
    }
    if (payload.kind === 'p2p-ping') {
      try { channel.send(JSON.stringify({ kind: 'p2p-pong', pingId: payload.pingId })); } catch (e) {}
      return;
    }
    if (payload.kind === 'p2p-pong') {
      const resolve = this._pendingPings.get(payload.pingId);
      if (resolve) { this._pendingPings.delete(payload.pingId); resolve(); }
      return;
    }

    if (payload.msgId) {
      try { channel.send(JSON.stringify({ kind: 'p2p-ack', msgId: payload.msgId })); } catch (e) {}
      if (this._recentMsgIds.has(payload.msgId)) return;
      this._rememberMsgId(payload.msgId);
    }
    this._onIncomingPayload?.(peerUid, payload);
  }

  /** 把一个JSON字符串发出去：够小就直接发；太大就拆成多片，配合背压
      控制依次发出（见 _sendWithBackpressure），接收端靠 p2p-chunk-begin/
      p2p-chunk/p2p-chunk-end 这三种框架消息重新拼出来（见 onmessage）。
      P2P层的ACK/去重只认最终拼好的那条业务payload，分片本身不需要
      单独确认——丢了一片，接收端拼不完整会直接整条丢弃，靠上层"没等到
      ack就重发"的机制整条重来，不做分片级别的重传（没必要，增加复杂度
      换不来什么实际收益）。
      entry 传进来是为了每发一片就刷新一次 lastActivityTs（见下面
      _sendWithBackpressure）——不然大文件在背压控制下分片发送可能持续
      超过 HEARTBEAT_IDLE_MS，心跳定时器会误以为连接"空闲"了、跑去发一个
      ping，而这个ping很可能排在一堆还没发完的分片后面（数据通道默认
      保序），迟迟发不出去/收不到pong，心跳超时判定就会把这条正在传输
      中、其实完全正常的连接强行reset掉——这正是"大文件传输容易断"、
      "传完了也没有回执"的根源：连接在ack真正送达之前就被自己人为掐断了。 */
  async _sendRaw(channel, str, entry) {
    if (str.length <= this.CHUNK_SIZE) {
      await this._sendWithBackpressure(channel, str, entry);
      return;
    }
    const xferId = 'x_' + Date.now().toString(36) + Math.random().toString(36).slice(2);
    const total = Math.ceil(str.length / this.CHUNK_SIZE);
    await this._sendWithBackpressure(channel, JSON.stringify({ kind: 'p2p-chunk-begin', xferId, total }), entry);
    for (let i = 0; i < total; i++) {
      const piece = str.slice(i * this.CHUNK_SIZE, (i + 1) * this.CHUNK_SIZE);
      await this._sendWithBackpressure(channel, JSON.stringify({ kind: 'p2p-chunk', xferId, seq: i, data: piece }), entry);
    }
    await this._sendWithBackpressure(channel, JSON.stringify({ kind: 'p2p-chunk-end', xferId }), entry);
  }

  /** 发送前先看一眼数据通道自己的发送缓冲区（bufferedAmount）——堆得太
      高就先等 bufferedamountlow 事件，不然连续发很多片，哪怕每片本身
      不大，也可能把发送缓冲区一口气灌爆，一样会抛 Failure to send data。 */
  _sendWithBackpressure(channel, str, entry) {
    return new Promise((resolve, reject) => {
      const trySend = () => {
        if (channel.readyState !== 'open') { reject(new Error('channel not open')); return; }
        if (channel.bufferedAmount > this.CHUNK_HIGH_WATERMARK) {
          channel.bufferedAmountLowThreshold = Math.floor(this.CHUNK_HIGH_WATERMARK / 2);
          channel.addEventListener('bufferedamountlow', trySend, { once: true });
          return;
        }
        try { channel.send(str); if (entry) entry.lastActivityTs = Date.now(); resolve(); }
        catch (e) { reject(e); }
      };
      trySend();
    });
  }

  /** 等对方ACK的超时时长——固定的短超时对大消息不现实：分片发送本身
      在背压控制下就可能要等一阵子，对方收完全部分片、拼好、解密、
      回ack也需要时间。按数据量给足够的宽限，同时有个下限（ACK_TIMEOUT_MS，
      构造时可调），网络本身的延迟（尤其走TURN中继多一跳）也需要基础的
      容忍空间，不能消息一小就卡死用最短的那档超时。 */
  _ackTimeoutFor(byteLength) {
    return Math.max(this.ACK_TIMEOUT_MS, 4000 + byteLength / 20); // 粗略按至少20字节/毫秒（约20KB/s）估算所需时间，宁可多等，不要提前判定失败
  }

  async connect(peerUid) {
    const entry = await this.ensurePeer(peerUid);
    if (!entry || entry.channelCreated) return;
    entry.channelCreated = true;
    this._attachChannel(peerUid, entry.pc.createDataChannel('chat'));
  }

  async handleSignal(peerUid, signal) {
    if (signal.t === 'reset') { this.cleanupPeer(peerUid); return; }
    const entry = await this.ensurePeer(peerUid);
    if (!entry) return;
    const pc = entry.pc;

    if (signal.t === 'sdp') {
      const desc = signal.description;
      const offerCollision = desc.type === 'offer' && (entry.makingOffer || pc.signalingState !== 'stable');
      entry.ignoreOffer = !entry.polite && offerCollision;
      if (entry.ignoreOffer) return;
      await pc.setRemoteDescription(desc);
      // 远端描述刚设置好——把之前因为"还没 setRemoteDescription"而缓下来的
      // ICE candidate 补着加进去，不然那些候选会被静默丢弃，可能导致这条
      // 路径本来能打通却打不通（尤其是候选到达早于/紧跟 offer 到达的情况）。
      if (entry.pendingCandidates.length) {
        const queued = entry.pendingCandidates.splice(0);
        for (const c of queued) {
          try { await pc.addIceCandidate(c); } catch (e) { console.warn('[BubbleP2P] 补加缓存的ICE候选失败', e); }
        }
      }
      if (desc.type === 'offer') {
        await pc.setLocalDescription();
        this.sendSignal(peerUid, { t: 'sdp', description: pc.localDescription });
      }
    } else if (signal.t === 'ice') {
      if (!pc.remoteDescription) {
        // 对方的 offer/answer 还没到（或者还没处理完），这个候选先存起来，
        // 等 setRemoteDescription 完成后再补加，不能直接 addIceCandidate——
        // 会抛错，而且这个候选就永久丢了。
        entry.pendingCandidates.push(signal.candidate);
        return;
      }
      try { await pc.addIceCandidate(signal.candidate); }
      catch (e) { if (!entry.ignoreOffer) console.warn('[BubbleP2P] 添加ICE候选失败', e); }
    } else {
      this._onCustomSignal?.(peerUid, signal);
    }
  }

  isChannelOpen(peerUid) {
    const entry = this.peers[peerUid];
    return !!(entry && entry.channel && entry.channel.readyState === 'open');
  }

  async sendViaChannel(peerUid, payload) {
    if (!this.isChannelOpen(peerUid)) return false;
    const entry = this.peers[peerUid];
    try { await this._sendRaw(entry.channel, JSON.stringify(payload), entry); return true; }
    catch (e) { console.warn('[BubbleP2P] 直连发送失败', e); return false; }
  }

  sendReliable(peerUid, payload) {
    if (!this.isChannelOpen(peerUid)) return Promise.resolve(false);
    const entry = this.peers[peerUid];
    const str = JSON.stringify(payload);
    return (async () => {
      try { await this._sendRaw(entry.channel, str, entry); }
      catch (e) { console.warn('[BubbleP2P] 直连发送失败', e); return false; }
      if (!payload.msgId) return true;

      return new Promise((resolve) => {
        const timer = setTimeout(() => {
          this._pendingAcks.delete(payload.msgId);
          this.resetPeerConnection(peerUid, true);
          resolve(false);
        }, this._ackTimeoutFor(str.length));
        this._pendingAcks.set(payload.msgId, () => { clearTimeout(timer); resolve(true); });
      });
    })();
  }

  _startHeartbeat(peerUid) {
    const entry = this.peers[peerUid];
    if (!entry) return;
    entry.heartbeatTimer = setInterval(async () => {
      const cur = this.peers[peerUid];
      if (!cur || !this.isChannelOpen(peerUid)) { clearInterval(entry.heartbeatTimer); return; }
      if (Date.now() - cur.lastActivityTs < this.HEARTBEAT_IDLE_MS) return;

      const pingId = 'hb_' + crypto.randomUUID();
      const ok = await new Promise((resolve) => {
        try { cur.channel.send(JSON.stringify({ kind: 'p2p-ping', pingId })); }
        catch (e) { resolve(false); return; }
        const timer = setTimeout(() => { this._pendingPings.delete(pingId); resolve(false); }, this.HEARTBEAT_TIMEOUT_MS);
        this._pendingPings.set(pingId, () => { clearTimeout(timer); resolve(true); });
      });

      if (!this.peers[peerUid]) return;
      if (ok) { this.peers[peerUid].heartbeatMissed = 0; this.peers[peerUid].lastActivityTs = Date.now(); }
      else {
        this.peers[peerUid].heartbeatMissed++;
        if (this.peers[peerUid].heartbeatMissed >= 2) this.resetPeerConnection(peerUid, true);
      }
    }, this.HEARTBEAT_IDLE_MS);
  }

  cleanupPeer(peerUid) {
    const entry = this.peers[peerUid];
    if (!entry) return;
    if (entry.heartbeatTimer) clearInterval(entry.heartbeatTimer);
    if (entry.disconnectGraceTimer) clearTimeout(entry.disconnectGraceTimer);
    try { entry.channel?.close(); entry.pc.close(); } catch (e) {}
    delete this.peers[peerUid];
    this._onPeerClosed?.(peerUid);
  }

  /** ICE 重启：链路异常但不想整个 PeerConnection 推倒重来时用（比如 TURN
      分配到期）。跟 resetPeerConnection 的区别：不销毁数据通道/已协商的
      媒体 m-line 历史，只是重新跑一遍 ICE 协商，刷新候选和（如果配置了
      TURN）新的中继分配。 */
  async restartIce(peerUid) {
    const entry = this.peers[peerUid];
    if (!entry) return false;
    try {
      entry.makingOffer = true;
      await entry.pc.setLocalDescription(await entry.pc.createOffer({ iceRestart: true }));
      this.sendSignal(peerUid, { t: 'sdp', description: entry.pc.localDescription });
      return true;
    } catch (e) {
      console.warn('[BubbleP2P] ICE重启失败', e);
      return false;
    } finally {
      entry.makingOffer = false;
    }
  }

  resetPeerConnection(peerUid, rebuild) {
    this.sendSignal(peerUid, { t: 'reset' });
    this.cleanupPeer(peerUid);
    if (rebuild) this.connect(peerUid);
  }

  sweepConnections() {
    const now = Date.now();
    Object.keys(this.peers).forEach((peerUid) => {
      const entry = this.peers[peerUid];
      if (!entry) return;
      if (now - entry.createdAt > this.HARD_MAX_MS) { this.resetPeerConnection(peerUid, false); return; }
      const st = entry.pc.connectionState;
      if (st === 'connected' && entry.channel?.readyState === 'open') {
        if (now - entry.lastActivityTs > this.IDLE_TIMEOUT_MS) this.resetPeerConnection(peerUid, false);
      } else if (st === 'connecting' || st === 'new') {
        if (now - entry.createdAt > this.STUCK_TIMEOUT_MS) this.resetPeerConnection(peerUid, false);
      }
    });
  }

  verifyConnectionsOnForeground() {
    Object.keys(this.peers).forEach(async (peerUid) => {
      if (!this.isChannelOpen(peerUid)) return;
      const entry = this.peers[peerUid];
      const pingId = 'hb_' + crypto.randomUUID();
      const ok = await new Promise((resolve) => {
        try { entry.channel.send(JSON.stringify({ kind: 'p2p-ping', pingId })); }
        catch (e) { resolve(false); return; }
        const timer = setTimeout(() => { this._pendingPings.delete(pingId); resolve(false); }, this.HEARTBEAT_TIMEOUT_MS);
        this._pendingPings.set(pingId, () => { clearTimeout(timer); resolve(true); });
      });
      if (!ok && this.peers[peerUid]) this.resetPeerConnection(peerUid, true);
    });
  }

  waitForConnection(peerUid, timeoutMs) {
    if (this.isChannelOpen(peerUid)) return Promise.resolve(true);
    this.connect(peerUid);
    return new Promise((resolve) => {
      const startTs = Date.now();
      const check = () => {
        if (this.isChannelOpen(peerUid)) { resolve(true); return; }
        if (Date.now() - startTs >= timeoutMs) { resolve(false); return; }
        setTimeout(check, 300);
      };
      check();
    });
  }

  disconnectAll() { Object.keys(this.peers).forEach((uid) => this.cleanupPeer(uid)); }

  getPeerConnection(peerUid) {
    const entry = this.peers[peerUid];
    return entry ? entry.pc : null;
  }
}

if (typeof window !== 'undefined') window.BubbleP2P = BubbleP2P;
if (typeof module !== 'undefined' && module.exports) module.exports = BubbleP2P;
