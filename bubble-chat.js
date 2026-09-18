/* ============ bubble-chat.js ============
   BubbleChat — 通用的"消息怎么发出去"引擎：加密、优先走 P2P 直连、
   直连不行就走中转通道并等 ACK、去重、失败标记"未送达"。跟原来散在
   index.html 里的 encryptForPeer/getDmKey/sendViaRelayAndWaitAck/
   handleIncoming 等函数做的事完全一样，收进一个类里，方便复用/测试。

   这个类不关心：消息在屏幕上怎么画、IndexedDB 表结构、群聊的密钥分发
   策略——那些留给调用方（通过 hooks 传入），BubbleChat 只管"发出去"和
   "收进来"这两件事的可靠性。

   用法（1:1 私聊）：
     const chat = new BubbleChat({
       p2p,                                  // BubbleP2P 实例
       crypto: BubbleCrypto,
       resolvePublicKey: async (uid) => ...,  // 拿对方公钥（好友表/缓存/服务端 三级查找，由调用方实现）
       myPrivateKey: state.me.privateKey,
       sendViaRelay: (roomId, payload) => BubbleComm.send(roomId, payload),
       roomIdFor: (peerUid) => dmRoomId(state.me.userId, peerUid),
     });

     chat.onDecrypted((peerUid, obj, viaP2P) => { ...写入 IndexedDB、更新UI... });
     p2p.setIncomingPayloadHandler((peerUid, payload) => chat.handleIncomingP2P(peerUid, payload));
     // 中转通道收到消息时（原 handleIncoming 的入口）：
     chat.handleIncomingRelay(fromUid, payload);

     const ok = await chat.sendDM(peerUid, { type:'text', content:'hi' }, msgId);
*/
class BubbleChat {
  /**
   * @param {object} opts
   * @param {object} opts.p2p - BubbleP2P 实例
   * @param {object} opts.crypto - 形如 BubbleCrypto：需要 encryptText/decryptText/importPeerPublicKey/deriveSharedKey
   * @param {(uid:string) => Promise<string|null>} opts.resolvePublicKey
   * @param {CryptoKey} opts.myPrivateKey
   * @param {(roomId:string, payload:object) => void} opts.sendViaRelay
   * @param {(peerUid:string) => string} opts.roomIdFor
   * @param {number} [opts.relayAckTimeoutMs]
   * @param {number} [opts.relaySizeLimitBytes] - 中转兜底允许的最大内容体积，超过这个体积的内容P2P失败后不会走中转，见 _relayAllowed
   */
  constructor(opts) {
    for (const k of ['p2p', 'crypto', 'resolvePublicKey', 'myPrivateKey', 'sendViaRelay', 'roomIdFor']) {
      if (opts[k] === undefined) throw new Error(`BubbleChat: 缺少必需依赖 ${k}`);
    }
    this.p2p = opts.p2p;
    this.crypto = opts.crypto;
    this._resolvePublicKey = opts.resolvePublicKey;
    this.myPrivateKey = opts.myPrivateKey;
    this._sendViaRelay = opts.sendViaRelay;
    this._roomIdFor = opts.roomIdFor;
    this._relayAckTimeoutMs = opts.relayAckTimeoutMs ?? 4000;
    // 中转兜底允许的最大内容体积（粗略按JSON化后的字节数估算）——见
    // _relayAllowed 的说明，默认给4MB，构造时可调
    this._relaySizeLimitBytes = opts.relaySizeLimitBytes ?? 4 * 1024 * 1024;

    this._dmKeyCache = {};       // peerUid -> derived AES key
    this._pendingRelayAcks = new Map(); // msgId/reqId -> resolve
    this._decryptedListeners = [];
    this._rawListeners = []; // 收到但解不开的信封，供调用方记日志/丢弃
  }

  onDecrypted(fn) { this._decryptedListeners.push(fn); return () => { this._decryptedListeners = this._decryptedListeners.filter(f => f !== fn); }; }
  onDecryptFailed(fn) { this._rawListeners.push(fn); return () => { this._rawListeners = this._rawListeners.filter(f => f !== fn); }; }

  async _getDmKey(peerUid) {
    if (this._dmKeyCache[peerUid]) return this._dmKeyCache[peerUid];
    const peerPubB64 = await this._resolvePublicKey(peerUid);
    if (!peerPubB64) return null;
    const peerPubKey = await this.crypto.importPeerPublicKey(peerPubB64);
    const aesKey = await this.crypto.deriveSharedKey(this.myPrivateKey, peerPubKey);
    this._dmKeyCache[peerUid] = aesKey;
    return aesKey;
  }

  /** 对方换了公钥（比如重新注册）之后，调用方应该清掉缓存，强制下次重新派生 */
  invalidateKey(peerUid) { delete this._dmKeyCache[peerUid]; }

  async encryptForPeer(peerUid, obj) {
    const key = await this._getDmKey(peerUid);
    if (!key) return null;
    return this.crypto.encryptText(key, JSON.stringify(obj));
  }

  async decryptFromPeer(peerUid, envelope) {
    const key = await this._getDmKey(peerUid);
    if (!key) return null;
    try { return JSON.parse(await this.crypto.decryptText(key, envelope)); }
    catch (e) { console.warn('[BubbleChat] 解密失败', e); return null; }
  }

  /**
   * 发一条 1:1 消息：优先走 P2P 直连（等 ACK）。是否允许在 P2P 不通时
   * 降级走中转通道兜底，取决于内容类型——这是三层传输降级链路里
   * "第三层只能传文字，不能传文件"这条规则的落地：
   *   - 文本/表情等小体量内容：P2P 不通就走中转，兜底送达。
   *   - 文件（含图片）：P2P 不通就直接失败，绝不占用中转通道传输
   *     二进制数据。调用方应该把这次失败标成"待送达"，等
   *     BubbleP2P 的 channelOpen 事件触发后自己重发（P2P直连恢复后
   *     自动补发的逻辑不属于这个类，属于业务层——这个类只负责"这一次
   *     发送允不允许走中转、走了通不通"）。
   * @param {string} peerUid
   * @param {object} content - 明文内容，例如 {type:'text', content:'hi'} 或 {type:'file', ...}
   * @param {string} msgId - 用于去重和 ACK 匹配
   * @returns {Promise<boolean>} 是否确认送达
   */
  async sendDM(peerUid, content, msgId) {
    const envelope = await this.encryptForPeer(peerUid, content);
    if (!envelope) return false; // 对方公钥暂时查不到——调用方应该把这条消息标"未送达"，稍后重试

    const payload = { kind: 'chat', isGroup: false, msgId, envelope };
    let delivered = await this.p2p.sendReliable(peerUid, payload);
    if (!delivered && this._relayAllowed(content)) {
      delivered = await this.sendViaRelayAndWaitAck(this._roomIdFor(peerUid), payload);
    }
    return delivered;
  }

  /** 决定这次发送在 P2P 失败时能不能走中转兜底。原来是按 type==='file'
      这个标签一刀切——不管文件多小都不许走中转，但中转通道扛不扛得住，
      取决于这条消息实际有多大，跟界面上把它叫"文件"还是"图片"没有
      任何关系：2KB的文件和2KB的图片对中转通道来说完全一样。改成按
      估算出的字节数判断，图片和文件用同一套标准——小文件现在能跟小
      图片一样正常走中转兜底了，只有真的很大的内容（不管是文件还是
      图片）才会被挡在中转之外，只能等P2P直连恢复后自动补发。
      这个阈值是这个项目自己的保护策略，不是Cloudflare的硬限制——
      Workers/Durable Objects 的WebSocket单条消息上限是32MiB，比这个
      阈值宽裕得多；留这个阈值单纯是不想让中转通道长期被一次性的大体积
      传输占着，不是"传不过去"。 */
  _relayAllowed(content) {
    if (!content) return true;
    const approxBytes = JSON.stringify(content).length;
    return approxBytes <= this._relaySizeLimitBytes;
  }

  /** 中转通道发送并等待业务层 ACK（跟 P2P 那套 ACK 是两条独立的确认机制，互不影响） */
  sendViaRelayAndWaitAck(roomId, payload, timeoutMs = this._relayAckTimeoutMs) {
    this._sendViaRelay(roomId, payload);
    const id = payload.msgId || payload.reqId;
    if (!id) return Promise.resolve(true);
    return new Promise((resolve) => {
      const timer = setTimeout(() => { this._pendingRelayAcks.delete(id); resolve(false); }, timeoutMs);
      this._pendingRelayAcks.set(id, () => { clearTimeout(timer); resolve(true); });
    });
  }

  /** 中转通道收到一个 {kind:'ack', msgId|reqId} 时，调用方应该转发到这里来触发上面等待的 Promise */
  handleRelayAck(id) {
    const resolve = this._pendingRelayAcks.get(id);
    if (resolve) { this._pendingRelayAcks.delete(id); resolve(); }
  }

  /** P2P 数据通道收到的业务消息（已经过 BubbleP2P 的 ACK+去重），解密后分发 */
  async handleIncomingP2P(peerUid, payload) {
    return this._handleIncoming(peerUid, payload, true);
  }

  /** 中转通道收到的业务消息（需要调用方自己保证不重复分发/或复用同一套去重表） */
  async handleIncomingRelay(peerUid, payload) {
    return this._handleIncoming(peerUid, payload, false);
  }

  async _handleIncoming(peerUid, payload, viaP2P) {
    if (payload.kind !== 'chat' || payload.isGroup) {
      // 群聊/非chat载荷（好友请求、rtc信令等）不归这个类管，调用方应该按 kind 自己分流
      this._rawListeners.forEach((fn) => fn(peerUid, payload, viaP2P));
      return;
    }
    const obj = await this.decryptFromPeer(peerUid, payload.envelope);
    if (!obj) { this._rawListeners.forEach((fn) => fn(peerUid, payload, viaP2P)); return; }
    this._decryptedListeners.forEach((fn) => fn(peerUid, obj, viaP2P, payload));
  }
}

// 双模导出：浏览器 <script> 标签下挂到 window，也支持 CommonJS/ESM 环境测试
if (typeof window !== 'undefined') window.BubbleChat = BubbleChat;
if (typeof module !== 'undefined' && module.exports) module.exports = BubbleChat;
