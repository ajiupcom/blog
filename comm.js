/* ============ comm.js ============
   BubbleComm — 与传输协议解耦的消息传递层。
   优化：修复 leaveRoom 后仍自动重连的关键 Bug；指数退避重连；
         支持 offMessage 注销回调；连接状态查询。
   通过 <script src="comm.js"> 加载，全局暴露 BubbleComm。 */
const BubbleComm = (() => {
  let SIGNAL_ENDPOINT = 'wss://ws.xxooe.com/signal';
  const TRANSPORT_MODE = { current: 'websocket' }; // 'loopback' | 'websocket'
  const MAX_RECONNECT_DELAY = 30000;

  /** 切换信令端点（部署到别的项目/环境时用），返回旧地址。跟 cf.js 的
      setApiBase 是同一个模式——库文件本身不该硬编码某一个具体项目的域名。 */
  function setSignalEndpoint(url) { const old = SIGNAL_ENDPOINT; SIGNAL_ENDPOINT = url; return old; }

  class LoopbackTransport {
    constructor() {
      this.channels = new Map(); // roomId -> BroadcastChannel
      this.listeners = [];
    }
    joinRoom(roomId) {
      if (this.channels.has(roomId)) return;
      const ch = new BroadcastChannel('bubbleim:' + roomId);
      ch.onmessage = (ev) => this.listeners.forEach(cb => cb(roomId, ev.data));
      this.channels.set(roomId, ch);
    }
    leaveRoom(roomId) {
      const ch = this.channels.get(roomId);
      if (ch) { ch.close(); this.channels.delete(roomId); }
    }
    send(roomId, payload) {
      this.joinRoom(roomId);
      this.channels.get(roomId).postMessage(payload);
    }
    onMessage(cb) { this.listeners.push(cb); }
    offMessage(cb) {
      const i = this.listeners.indexOf(cb);
      if (i !== -1) this.listeners.splice(i, 1);
    }
    isConnected(roomId) { return this.channels.has(roomId); }
  }

  class WebSocketTransport {
    constructor() {
      this.sockets = new Map();       // roomId -> WebSocket
      this.listeners = [];
      this.myUserId = null;
      this.authToken = null;          // 可选——有登录体系的项目用来在WS握手时证明身份（见 init）
      this.intentionallyClosed = new Set(); // 主动离开的房间，禁止自动重连
      this.reconnectAttempts = new Map();   // roomId -> 当前重试次数
    }
    init(myUserId, authToken) { this.myUserId = myUserId; this.authToken = authToken || null; }

    joinRoom(roomId) {
      if (this.sockets.has(roomId)) return;
      this.intentionallyClosed.delete(roomId); // 重新加入时清除主动关闭标记
      let url = `${SIGNAL_ENDPOINT}?room=${encodeURIComponent(roomId)}&uid=${encodeURIComponent(this.myUserId)}`;
      if (this.authToken) url += `&token=${encodeURIComponent(this.authToken)}`;
      const ws = new WebSocket(url);

      ws.onmessage = (ev) => {
        let payload;
        try { payload = JSON.parse(ev.data); } catch { return; }
        this.listeners.forEach(cb => cb(roomId, payload));
      };

      ws.onopen = () => {
        // 连接成功，重置退避计数
        this.reconnectAttempts.delete(roomId);
      };

      ws.onclose = () => {
        this.sockets.delete(roomId);
        // 关键：只有非主动离开的房间才重连
        if (this.intentionallyClosed.has(roomId)) return;
        const attempts = (this.reconnectAttempts.get(roomId) || 0) + 1;
        this.reconnectAttempts.set(roomId, attempts);
        // 指数退避：1s, 2s, 4s, 8s ... 上限 30s
        const delay = Math.min(1000 * Math.pow(2, attempts - 1), MAX_RECONNECT_DELAY);
        setTimeout(() => {
          if (!this.sockets.has(roomId) && !this.intentionallyClosed.has(roomId)) {
            this.joinRoom(roomId);
          }
        }, delay);
      };

      ws.onerror = () => {
        // onerror 之后通常紧跟 onclose，重连逻辑在 onclose 中处理
        console.warn(`[Comm] websocket error for room ${roomId}`);
      };

      this.sockets.set(roomId, ws);
    }

    leaveRoom(roomId) {
      // 先标记主动关闭，再关闭连接 — onclose 中会检查此标记并放弃重连
      this.intentionallyClosed.add(roomId);
      this.reconnectAttempts.delete(roomId);
      const ws = this.sockets.get(roomId);
      if (ws) {
        ws.onclose = null; // 直接解绑，避免任何重连逻辑
        ws.close();
        this.sockets.delete(roomId);
      }
    }

    send(roomId, payload) {
      this.joinRoom(roomId);
      const ws = this.sockets.get(roomId);
      const data = JSON.stringify(payload);
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(data);
      } else {
        ws.addEventListener('open', () => ws.send(data), { once: true });
      }
    }

    onMessage(cb) { this.listeners.push(cb); }
    offMessage(cb) {
      const i = this.listeners.indexOf(cb);
      if (i !== -1) this.listeners.splice(i, 1);
    }
    isConnected(roomId) {
      const ws = this.sockets.get(roomId);
      return !!(ws && ws.readyState === WebSocket.OPEN);
    }
  }

  const loopback = new LoopbackTransport();
  const websocket = new WebSocketTransport();

  function activeTransport() {
    return TRANSPORT_MODE.current === 'websocket' ? websocket : loopback;
  }

  return {
    init(myUserId, authToken) { websocket.init(myUserId, authToken); },
    setSignalEndpoint,
    setTransportMode(mode) { TRANSPORT_MODE.current = mode; },
    getTransportMode() { return TRANSPORT_MODE.current; },
    joinRoom(roomId) { activeTransport().joinRoom(roomId); },
    leaveRoom(roomId) { activeTransport().leaveRoom(roomId); },
    send(roomId, payload) { activeTransport().send(roomId, payload); },
    onMessage(cb) {
      // 只向当前活跃传输注册，避免无效监听累积
      activeTransport().onMessage(cb);
    },
    offMessage(cb) {
      loopback.offMessage(cb);
      websocket.offMessage(cb);
    },
    isConnected(roomId) { return activeTransport().isConnected(roomId); }
  };
})();
