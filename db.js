/* BubbleDB — 用于账号、好友、会话、消息、设置的 IndexedDB 持久化存储
   多账号安全：好友/会话/消息使用 ownerId 复合键进行隔离。
   v2: 新增 messages 复合索引 [ownerId, from]，加速未送达消息扫描。
   v3: 新增 searchLocal()——顶栏本地搜索用，不涉及 schema 变化。 */
const BubbleDB = (() => {
  const DB_NAME = 'bubbleim_v3';
  const DB_VER = 2;
  let dbPromise = null;

  function open() {
    if (dbPromise) return dbPromise;
    dbPromise = new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, DB_VER);
      req.onupgradeneeded = (e) => {
        const db = e.target.result;
        const tx = e.target.transaction;
        /* ---- v1 初始 schema ---- */
        if (e.oldVersion < 1) {
          if (!db.objectStoreNames.contains('accounts')) {
            db.createObjectStore('accounts', { keyPath: 'userId' });
          }
          if (!db.objectStoreNames.contains('friends')) {
            const s = db.createObjectStore('friends', { keyPath: '_key' });
            s.createIndex('ownerId', 'ownerId', { unique: false });
          }
          if (!db.objectStoreNames.contains('convos')) {
            const s = db.createObjectStore('convos', { keyPath: '_key' });
            s.createIndex('ownerId', 'ownerId', { unique: false });
          }
          if (!db.objectStoreNames.contains('messages')) {
            const s = db.createObjectStore('messages', { keyPath: 'msgId' });
            s.createIndex('convId', 'convId', { unique: false });
            s.createIndex('ownerConv', 'ownerConv', { unique: false });
            s.createIndex('expiresAt', 'expiresAt', { unique: false });
          }
          if (!db.objectStoreNames.contains('settings')) {
            db.createObjectStore('settings', { keyPath: 'key' });
          }
        }
        /* ---- v2 升级：messages 增加复合索引，加速未送达扫描 ---- */
        if (e.oldVersion < 2) {
          const msgStore = tx.objectStore('messages');
          if (!msgStore.indexNames.contains('ownerFrom')) {
            msgStore.createIndex('ownerFrom', ['ownerId', 'from'], { unique: false });
          }
        }
      };
      req.onsuccess = () => {
        const db = req.result;
        db.onclose = () => { dbPromise = null; };
        db.onerror = (ev) => console.error('[BubbleDB] database error:', ev.target.error);
        resolve(db);
      };
      req.onerror = () => reject(req.error);
      req.onblocked = () => console.warn('[BubbleDB] upgrade blocked — close other tabs');
    });
    return dbPromise;
  }

  /** 关闭数据库连接（下次操作会自动重开）。 */
  function close() {
    if (dbPromise) {
      dbPromise.then(db => db.close()).catch(() => {});
      dbPromise = null;
    }
  }

  /** 单 store 事务，返回 objectStore。保持向后兼容。 */
  function tx(store, mode = 'readonly') {
    return open().then(db => {
      const t = db.transaction(store, mode);
      t.onabort = () => console.error('[BubbleDB] transaction aborted:', t.error);
      return t.objectStore(store);
    });
  }

  /**
   * 多 store 原子事务辅助：在同一个事务内执行操作，避免跨事务不一致。
   * callback 接收 stores 映射对象 { storeName: IDBObjectStore }，返回值会被透传。
   */
  function withTx(stores, mode, callback) {
    const names = Array.isArray(stores) ? stores : [stores];
    return open().then(db => new Promise((resolve, reject) => {
      const t = db.transaction(names, mode);
      t.onabort = () => reject(t.error || new Error('transaction aborted'));
      t.oncomplete = () => resolve(result);
      t.onerror = () => reject(t.error);
      const storeMap = {};
      for (const name of names) storeMap[name] = t.objectStore(name);
      let result;
      try {
        result = callback(storeMap, t);
      } catch (err) {
        reject(err);
      }
    }));
  }

  function reqToPromise(req) {
    return new Promise((resolve, reject) => {
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  function friendKey(ownerId, userId) { return ownerId + '::' + userId; }
  function convoKey(ownerId, id) { return ownerId + '::' + id; }
  function ownerConvKey(ownerId, convId) { return ownerId + '::' + convId; }

  // ---- 系统设置 ----
  async function getSetting(key, def) {
    const store = await tx('settings');
    const v = await reqToPromise(store.get(key));
    return v ? v.value : def;
  }
  async function setSetting(key, value) {
    const store = await tx('settings', 'readwrite');
    await reqToPromise(store.put({ key, value }));
  }

  // ---- 多账号管理 ----
  async function listAccounts() {
    const store = await tx('accounts');
    return reqToPromise(store.getAll());
  }
  async function getAccountById(userId) {
    const store = await tx('accounts');
    return reqToPromise(store.get(userId));
  }
  async function saveAccount(acc) {
    const store = await tx('accounts', 'readwrite');
    await reqToPromise(store.put(acc));
  }
  async function deleteAccount(userId) {
    const store = await tx('accounts', 'readwrite');
    await reqToPromise(store.delete(userId));
  }
  async function getActiveUserId() {
    return getSetting('activeUserId', null);
  }
  async function setActiveUserId(userId) {
    return setSetting('activeUserId', userId);
  }
  async function getAccount() {
    const uid = await getActiveUserId();
    if (!uid) return null;
    return getAccountById(uid);
  }

  // ---- 好友列表 (复合主键 ownerId::userId) ----
  async function listFriends() {
    const me = await getAccount();
    if (!me) return [];
    const store = await tx('friends');
    const idx = store.index('ownerId');
    return reqToPromise(idx.getAll(me.userId));
  }
  async function upsertFriend(f) {
    const me = await getAccount();
    if (!me) return;
    f.ownerId = me.userId;
    f._key = friendKey(me.userId, f.userId);
    const store = await tx('friends', 'readwrite');
    await reqToPromise(store.put(f));
  }
  async function deleteFriend(userId) {
    const me = await getAccount();
    if (!me) return;
    const store = await tx('friends', 'readwrite');
    await reqToPromise(store.delete(friendKey(me.userId, userId)));
  }

  // ---- 会话列表 (复合主键 ownerId::id) ----
  async function listConvos() {
    const me = await getAccount();
    if (!me) return [];
    const store = await tx('convos');
    const idx = store.index('ownerId');
    return reqToPromise(idx.getAll(me.userId));
  }
  async function upsertConvo(c) {
    const me = await getAccount();
    if (!me) return;
    c.ownerId = me.userId;
    c._key = convoKey(me.userId, c.id);
    const store = await tx('convos', 'readwrite');
    await reqToPromise(store.put(c));
  }
  async function deleteConvo(id) {
    const me = await getAccount();
    if (!me) return;
    // 单事务内同时删会话和消息，保证原子性
    await withTx(['convos', 'messages'], 'readwrite', async ({ convos, messages }) => {
      await reqToPromise(convos.delete(convoKey(me.userId, id)));
      const idx = messages.index('ownerConv');
      const msgs = await reqToPromise(idx.getAll(ownerConvKey(me.userId, id)));
      for (const m of msgs) {
        messages.delete(m.msgId);
      }
    });
  }
  async function deleteMessagesForConvo(convId) {
    const me = await getAccount();
    if (!me) return;
    const store = await tx('messages', 'readwrite');
    const idx = store.index('ownerConv');
    const msgs = await reqToPromise(idx.getAll(ownerConvKey(me.userId, convId)));
    for (const m of msgs) {
      store.delete(m.msgId);
    }
  }

  // ---- 消息列表 (基于 ownerConv = ownerId::convId 作用域隔离) ----
  async function getMessage(msgId) {
    // msgId 本身已是全局唯一（crypto.randomUUID()），用于实时消息去重
    const store = await tx('messages');
    return reqToPromise(store.get(msgId));
  }
  async function listMessages(convId, limit) {
    const me = await getAccount();
    if (!me) return [];
    const store = await tx('messages');
    const idx = store.index('ownerConv');
    const msgs = await reqToPromise(idx.getAll(ownerConvKey(me.userId, convId)));
    msgs.sort((a, b) => (a.createdAt || 0) - (b.createdAt || 0));
    return typeof limit === 'number' ? msgs.slice(-limit) : msgs;
  }
  async function addMessage(msg) {
    const me = await getAccount();
    if (!me) return;
    msg.ownerId = me.userId;
    msg.ownerConv = ownerConvKey(me.userId, msg.convId);
    const store = await tx('messages', 'readwrite');
    await reqToPromise(store.put(msg));
  }

  // ---- 单条/多条消息删除（仅本机，不通知对方） ----
  async function deleteMessage(msgId) {
    const store = await tx('messages', 'readwrite');
    await reqToPromise(store.delete(msgId));
  }
  async function deleteMessages(msgIds) {
    const store = await tx('messages', 'readwrite');
    for (const id of msgIds) {
      store.delete(id);
    }
  }

  // ---- 消息送达状态 ----
  async function markMessageDelivered(msgId, delivered = true) {
    const store = await tx('messages', 'readwrite');
    const msg = await reqToPromise(store.get(msgId));
    if (!msg) return;
    msg.delivered = delivered;
    await reqToPromise(store.put(msg));
  }

  /**
   * 列出当前用户发出但未送达的消息，按发送时间升序。
   * 使用 v2 新增的 [ownerId, from] 复合索引，避免全表扫描。
   */
  async function listUndeliveredMessages() {
    const me = await getAccount();
    if (!me) return [];
    const store = await tx('messages');
    const idx = store.index('ownerFrom');
    // 复合索引精确匹配 ownerId + from（都是 me.userId）
    const range = IDBKeyRange.only([me.userId, me.userId]);
    const msgs = await reqToPromise(idx.getAll(range));
    // delivered 字段无索引，在结果集内过滤（数据量已大幅缩小）
    return msgs
      .filter(m => m.delivered === false)
      .sort((a, b) => (a.createdAt || 0) - (b.createdAt || 0));
  }

  /**
   * 清理过期消息 + 自动解散 TTL 超时的临时群组。
   * 优化：(1) 过期消息用 expiresAt 索引范围查询而非全表扫描；
   *       (2) 会话和消息清理放在同一多 store 事务，避免嵌套事务自动提交 bug。
   */
  async function purgeExpiredMessages() {
    const now = Date.now();
    // 第一步：用索引范围删除所有过期消息（单事务，高效）
    await withTx(['messages'], 'readwrite', async ({ messages }) => {
      const idx = messages.index('expiresAt');
      const range = IDBKeyRange.upperBound(now);
      const expired = await reqToPromise(idx.getAll(range));
      for (const m of expired) {
        messages.delete(m.msgId);
      }
    });
    // 第二步：扫描超时临时会话，在单事务内同时删会话和其消息
    await withTx(['convos', 'messages'], 'readwrite', async ({ convos, messages }) => {
      const all = await reqToPromise(convos.getAll());
      for (const c of all) {
        if (c.ephemeral && c.ttlMs && c.createdAt && (c.createdAt + c.ttlMs) < now) {
          convos.delete(c._key);
          const idx = messages.index('ownerConv');
          const oc = ownerConvKey(c.ownerId, c.id);
          const msgs = await reqToPromise(idx.getAll(oc));
          for (const m of msgs) {
            messages.delete(m.msgId);
          }
        }
      }
    });
  }

  /**
   * 顶栏本地搜索（v3 新增）：纯 IndexedDB 查询，不发任何网络请求。
   * 跟"添加好友"用的云端搜索（BubbleCF.searchUsers，查 D1 users 表）是
   * 两件完全独立的事——这个函数只搜本地已经有的数据：好友的昵称/备注、
   * 以及本地已经存下来的聊天记录内容。
   * @returns {Promise<{friends: object[], messages: {convId:string, msg:object}[]}>}
   */
  async function searchLocal(query) {
    const q = (query || '').trim().toLowerCase();
    if (!q) return { friends: [], messages: [] };

    const [friends, convos] = await Promise.all([listFriends(), listConvos()]);
    const matchedFriends = friends.filter((f) =>
      (f.nickname && f.nickname.toLowerCase().includes(q)) ||
      (f.remark && f.remark.toLowerCase().includes(q))
    );

    // 逐个会话扫描本地消息——量级是"一个人自己聊天记录的总条数"，不是
    // 全体用户数据，个人使用场景下全量扫描完全没问题；真要优化也应该等
    // 实测遇到卡顿再加（比如按会话建一份倒排索引），现在加是过度设计。
    const matchedMessages = [];
    for (const c of convos) {
      const msgs = await listMessages(c.id);
      for (const m of msgs) {
        const text = m.type === 'file' ? (m.name || '') : (m.content || '');
        if (text && text.toLowerCase().includes(q)) matchedMessages.push({ convId: c.id, msg: m });
      }
    }
    matchedMessages.sort((a, b) => (b.msg.createdAt || 0) - (a.msg.createdAt || 0));

    return { friends: matchedFriends, messages: matchedMessages };
  }

  async function deleteFriendAndChat(friendUserId) {
    await deleteFriend(friendUserId);
    await deleteConvo(friendUserId);
  }

  return {
    open, close,
    getSetting, setSetting,
    listAccounts, getAccountById, saveAccount, deleteAccount,
    getActiveUserId, setActiveUserId, getAccount,
    listFriends, upsertFriend, deleteFriend, deleteFriendAndChat,
    listConvos, upsertConvo, deleteConvo, deleteMessagesForConvo,
    listMessages, addMessage, getMessage, deleteMessage, deleteMessages,
    markMessageDelivered, listUndeliveredMessages,
    purgeExpiredMessages, searchLocal
  };
})();
