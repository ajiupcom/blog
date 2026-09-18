/* ============ bubble-call.js ============
   BubbleCall — 通用的语音/视频通话状态机，从原 index.html 里的 Call IIFE
   提炼出来：呼叫/接听/拒接/挂断/切换摄像头/静音这套状态转换和信令收发
   逻辑完全保留，但不再直接操作 DOM（不再有 renderCallOverlay() 这种
   耦合），改成通过 hooks 把"现在的通话状态"通知给外层，DOM 渲染完全
   交给调用方——这样它才能被别的 UI（甚至别的项目）复用。

   依赖：一个 BubbleP2P 实例（见 bubble-p2p.js）。

   用法：
     const call = new BubbleCall({
       p2p,
       getConnectionMode: () => state.connectionModeUi,
       isRelayCallsAllowed: () => true,
       onStateChange: (snapshot) => renderCallOverlay(snapshot), // snapshot 见下方 _snapshot()
       onRemoteStream: (stream) => { ...把 stream 接到 <video>/<audio> 上... },
       onToast: (msg) => showToast(msg),
       onLogCall: async (peerUid, video, direction, status) => logCallMessage(...), // 返回 callMsgId
       onUpdateCallLog: (callMsgId, convId, patch) => updateCallMessage(callMsgId, convId, patch),
       ensureConvo: (peerUid) => ensureConvoWithFriend(peerUid), // 返回 {id}
     });

     call.start(peerUid, video);      // 发起
     call.accept();                   // 接听当前来电
     call.decline();                  // 拒接
     call.hangup();                   // 挂断/取消
     call.toggleMute(); call.toggleCamera(); call.flipCamera();
*/
class BubbleCall {
  constructor(opts) {
    if (!opts || !opts.p2p) throw new Error('BubbleCall: 需要传入 p2p (BubbleP2P 实例)');
    this.p2p = opts.p2p;
    this._getConnectionMode = opts.getConnectionMode || (() => 'p2p');
    this._isRelayCallsAllowed = opts.isRelayCallsAllowed || (() => true);
    this._onStateChange = opts.onStateChange || (() => {});
    this._onRemoteStream = opts.onRemoteStream || (() => {});
    this._onToast = opts.onToast || (() => {});
    this._onLogCall = opts.onLogCall || (async () => null);
    this._onUpdateCallLog = opts.onUpdateCallLog || (() => {});
    this._ensureConvo = opts.ensureConvo || (async (peerUid) => ({ id: peerUid }));
    this._t = opts.t || ((zh, en) => zh); // 简单的国际化钩子，不传就固定用第一个参数
    this._callTimeoutMs = opts.callTimeoutMs ?? 30 * 1000;
    this._callTimeoutTimer = null;

    this.currentCameraFacing = 'user';
    this._callState = null;
    this._remoteStream = null;
    this._timerId = null;

    this.p2p.setCustomSignalHandler((peerUid, signal) => this._handleCallSignal(peerUid, signal));
    this.p2p.setRemoteTrackHandler((peerUid, ev) => this._handleRemoteTrack(peerUid, ev));
    this.p2p.setPeerClosedHandler((peerUid) => {
      if (this._callState && this._callState.peerUid === peerUid) {
        this._onToast(this._t('连接已断开，通话结束', 'Connection lost — call ended'));
        this._teardown();
      }
    });
  }

  /** 供外层渲染用的、不含 pc/stream 等不可序列化对象的状态快照 */
  _snapshot() {
    if (!this._callState) return null;
    const { peerUid, video, status, direction, startedAt } = this._callState;
    return { peerUid, video, status, direction, startedAt, remoteStream: this._remoteStream, localStream: this._callState.localStream };
  }

  _emit() { this._onStateChange(this._snapshot()); }

  isInCall() { return !!this._callState; }

  async start(peerUid, video) {
    if (this._callState) { this._onToast(this._t('当前已有通话，请先挂断', 'Already in a call — hang up first')); return; }

    const relayCallsOk = this._getConnectionMode() === 'relay' && this._isRelayCallsAllowed();
    if (!relayCallsOk) {
      if (!this.p2p.isChannelOpen(peerUid)) {
        this._onToast(this._t('正在建立直连…', 'Establishing direct connection…'));
        const ok = await this.p2p.waitForConnection(peerUid, 6000);
        if (!ok) { this._onToast(this._t('尚未建立直连，暂时无法通话', "No direct connection yet — can't start a call")); return; }
      }
    } else {
      await this.p2p.connect(peerUid);
    }

    const pc = this.p2p.getPeerConnection(peerUid);
    if (!pc) { this._onToast(this._t('连接异常，请重试', 'Connection error — try again')); return; }

    let localStream;
    try {
      localStream = await navigator.mediaDevices.getUserMedia({ audio: true, video: video ? { facingMode: this.currentCameraFacing } : false });
    } catch (e) {
      this._onToast(this._t('无法获取摄像头/麦克风权限', 'Camera/microphone permission denied'));
      return;
    }

    const convo = await this._ensureConvo(peerUid);
    this._callState = {
      peerUid, video, status: 'calling', direction: 'outgoing', convId: convo.id, callMsgId: null,
      localStream, pc, senders: [], startedAt: null,
    };
    localStream.getTracks().forEach((track) => this._callState.senders.push(pc.addTrack(track, localStream)));
    this.p2p.sendSignal(peerUid, { t: 'call-invite', video });
    this._callState.callMsgId = await this._onLogCall(peerUid, video, 'outgoing', 'ringing');
    this._emit();

    // 呼叫超时：30秒没等到对方的 call-accept/call-decline/call-busy，
    // 就当作"未接听"自动挂断，别让呼叫方一直卡在"呼叫中"。放在客户端
    // 而不是服务端 DO 里判断，是因为这条信令本身是端到端加密的，服务端
    // 看不懂内容、也就没法判断"这通电话有没有被接听"。
    this._callTimeoutTimer = setTimeout(() => {
      if (this._callState && this._callState.peerUid === peerUid && this._callState.status === 'calling') {
        this.p2p.sendSignal(peerUid, { t: 'call-end' }); // 告诉对方"我不等了"，即便TA刚好这时候点了接听
        this._teardown('missed');
      }
    }, this._callTimeoutMs);
  }

  async accept() {
    if (!this._callState || this._callState.status !== 'incoming') return;

    // 业务层的"接听"信令是通话是否接通的权威判断依据，不是 WebRTC 的 ICE
    // 连接状态——所以先把这条信令发出去、把本地UI切成"通话中"，再去做
    // getUserMedia/addTrack（会触发媒体SDP重新协商，这部分有几百毫秒的
    // 延迟很正常）。这样对方（呼叫方）能第一时间知道"接通了"，不用等
    // 音视频轨道和ICE都协商完，UI才会显得"卡在呼叫中"。
    // 注意：这条信令跟聊天消息一样，是端到端加密后经个人信箱房间转发
    // 的密文——服务端只转发不解密，看不到"谁接听了谁的电话"这类信息。
    this._callState.status = 'active';
    this._callState.startedAt = Date.now();
    this._startTimer();
    this.p2p.sendSignal(this._callState.peerUid, { t: 'call-accept' });
    this._emit();

    let localStream;
    try {
      localStream = await navigator.mediaDevices.getUserMedia({ audio: true, video: this._callState.video ? { facingMode: this.currentCameraFacing } : false });
    } catch (e) {
      this._onToast(this._t('无法获取摄像头/麦克风权限', 'Camera/microphone permission denied'));
      // 业务信令已经发出去了（对方已经认为"接通"），这里不能再走 decline()
      // 那套"拒接"流程（会让对方以为是被拒绝，而不是我这边设备故障）——
      // 直接挂断更准确地反映"接通后设备出错，无法继续"这个情况。
      this.hangup();
      return;
    }
    this._callState.localStream = localStream;
    localStream.getTracks().forEach((track) => this._callState.senders.push(this._callState.pc.addTrack(track, localStream)));
    this._emit();
  }

  decline() {
    if (!this._callState) return;
    this.p2p.sendSignal(this._callState.peerUid, { t: 'call-decline' });
    this._teardown('declined');
  }

  hangup() {
    if (!this._callState) return;
    this.p2p.sendSignal(this._callState.peerUid, { t: 'call-end' });
    this._teardown(this._callState.status === 'calling' ? 'cancelled' : undefined);
  }

  async flipCamera() {
    if (!this._callState || !this._callState.video || !this._callState.localStream) return;
    const nextFacing = this.currentCameraFacing === 'user' ? 'environment' : 'user';
    const oldTrack = this._callState.localStream.getVideoTracks()[0];
    oldTrack?.stop();
    let newStream;
    try {
      newStream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: { exact: nextFacing } } });
    } catch (e) {
      try {
        newStream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: nextFacing } });
      } catch (e2) {
        this._onToast(this._t('切换摄像头失败', 'Failed to switch camera'));
        if (oldTrack) this._callState.localStream.addTrack(oldTrack);
        return;
      }
    }
    this.currentCameraFacing = nextFacing;
    const newTrack = newStream.getVideoTracks()[0];
    const sender = this._callState.senders.find((s) => s.track && s.track.kind === 'video');
    if (sender) await sender.replaceTrack(newTrack);
    this._callState.localStream.removeTrack(oldTrack);
    this._callState.localStream.addTrack(newTrack);
    // 不重新 emit 整个状态——localStream 还是同一个对象引用，外层 <video> 的
    // srcObject 不用重新赋值，浏览器会自动接着播新轨道。
  }

  toggleMute() {
    const track = this._callState?.localStream?.getAudioTracks()[0];
    if (!track) return false;
    track.enabled = !track.enabled;
    return !track.enabled; // 返回"现在是否静音"，方便外层更新按钮图标
  }

  toggleCamera() {
    const track = this._callState?.localStream?.getVideoTracks()[0];
    if (!track) return false;
    track.enabled = !track.enabled;
    return !track.enabled; // 返回"现在摄像头是否关闭"
  }

  _teardown(status) {
    if (!this._callState) return;
    if (this._timerId) clearInterval(this._timerId);
    if (this._callTimeoutTimer) { clearTimeout(this._callTimeoutTimer); this._callTimeoutTimer = null; }
    if (!status) status = this._callState.status === 'active' ? 'ended' : (this._callState.direction === 'outgoing' ? 'cancelled' : 'missed');
    const duration = this._callState.startedAt ? Math.round((Date.now() - this._callState.startedAt) / 1000) : 0;
    this._onUpdateCallLog(this._callState.callMsgId, this._callState.convId, { status, duration });
    this._callState.localStream?.getTracks().forEach((tr) => tr.stop());
    this._callState.senders.forEach((sender) => { try { this._callState.pc.removeTrack(sender); } catch (e) {} });
    this._callState = null;
    this._remoteStream = null;
    this._emit();
  }

  _startTimer() {
    this._timerId = setInterval(() => this._emit(), 1000);
  }

  _handleCallSignal(peerUid, signal) {
    if (signal.t === 'call-invite') {
      if (this._callState) { this.p2p.sendSignal(peerUid, { t: 'call-busy' }); return; }
      this._callState = {
        peerUid, video: !!signal.video, status: 'incoming', direction: 'incoming', convId: null, callMsgId: null,
        localStream: null, pc: this.p2p.getPeerConnection(peerUid), senders: [], startedAt: null,
      };
      this._ensureConvo(peerUid).then((convo) => {
        if (!this._callState || this._callState.peerUid !== peerUid) return;
        this._callState.convId = convo.id;
        this._onLogCall(peerUid, this._callState.video, 'incoming', 'ringing').then((msgId) => {
          if (this._callState && this._callState.peerUid === peerUid) this._callState.callMsgId = msgId;
        });
      });
      this._emit();
    } else if (signal.t === 'call-accept') {
      if (this._callState && this._callState.peerUid === peerUid && this._callState.status === 'calling') {
        if (this._callTimeoutTimer) { clearTimeout(this._callTimeoutTimer); this._callTimeoutTimer = null; }
        this._callState.status = 'active';
        this._callState.startedAt = Date.now();
        this._startTimer();
        this._emit();
      }
    } else if (signal.t === 'call-decline') {
      if (this._callState && this._callState.peerUid === peerUid) {
        this._onToast(this._t('对方拒绝了通话', 'Call declined'));
        this._teardown('declined');
      }
    } else if (signal.t === 'call-busy') {
      if (this._callState && this._callState.peerUid === peerUid) {
        this._onToast(this._t('对方正忙', 'They are on another call'));
        this._teardown('busy');
      }
    } else if (signal.t === 'call-end') {
      if (this._callState && this._callState.peerUid === peerUid) {
        this._onToast(this._t('通话已结束', 'Call ended'));
        this._teardown();
      }
    }
  }

  _handleRemoteTrack(peerUid, ev) {
    if (!this._callState || this._callState.peerUid !== peerUid) return;
    this._remoteStream = ev.streams[0] || this._remoteStream;
    this._onRemoteStream(this._remoteStream);
    this._emit();
  }
}

if (typeof window !== 'undefined') window.BubbleCall = BubbleCall;
if (typeof module !== 'undefined' && module.exports) module.exports = BubbleCall;
