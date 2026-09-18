/* ============ bubble-presence.js ============
   BubblePresence — 事件驱动的好友在线状态，配合 signalRoom.js (v2) 的
   "个人信箱房间"协议。取代原来 index.html 里的 p2pConnectSweep()——
   那个函数每隔几秒无条件遍历全部好友尝试建 P2P，不管对方在不在线；
   现在改成：只有服务端明确告诉你"这个好友上线了"，才去尝试建连接。

   用法：
     const presence = new BubblePresence({
       transport: BubbleComm,      // 需要 send(roomId, payload) / onMessage(cb)
       myUserId: acc.userId,
     });
     presence.onFriendOnline((uid) => P2P.connect(uid));
     presence.onFriendOffline((uid) => { ...更新好友列表上的在线小圆点... });
     presence.announce(state.friends.map(f => f.userId)); // 登录后调用一次
                                                            // 好友列表变化（加了新好友）时也应该重新调用一次

   注意：这个模块假定"个人信箱房间"就是 BubbleComm.joinRoom(myUserId) 这
   个已有约定——也就是说，只要 enterApp() 里那行 BubbleComm.joinRoom(state.me.userId)
   还在，这里不需要额外建立连接，只是在这条已有连接上多发一条控制帧、
   多监听几种消息类型。
*/
class BubblePresence {
  constructor({ transport, myUserId }) {
    if (!transport || typeof transport.send !== 'function') throw new Error('BubblePresence: 需要 transport.send(roomId, payload)');
    this.transport = transport;
    this.myUserId = myUserId;
    this.onlineFriendIds = new Set();
    this._onlineListeners = [];
    this._offlineListeners = [];

    transport.onMessage((roomId, payload) => {
      if (!payload || roomId !== myUserId) return; // 只处理发到"我自己信箱"里的控制帧
      if (payload.kind === 'friend_online') this._markOnline(payload.from);
      else if (payload.kind === 'friend_offline') this._markOffline(payload.from);
      else if (payload.kind === 'friends_online_snapshot') {
        (payload.ids || []).forEach((uid) => this._markOnline(uid));
      }
    });
  }

  onFriendOnline(fn) { this._onlineListeners.push(fn); return () => { this._onlineListeners = this._onlineListeners.filter(f => f !== fn); }; }
  onFriendOffline(fn) { this._offlineListeners.push(fn); return () => { this._offlineListeners = this._offlineListeners.filter(f => f !== fn); }; }

  isOnline(uid) { return this.onlineFriendIds.has(uid); }

  /** 登录后 / 好友列表变化后调用：把当前完整的好友ID列表告诉服务端，
      让服务端去逐个通知、并把已经在线的好友一次性回推给我 */
  announce(friendIds) {
    this.transport.send(this.myUserId, { kind: 'announce_friends', ids: friendIds, selfUid: this.myUserId });
  }

  _markOnline(uid) {
    if (this.onlineFriendIds.has(uid)) return;
    this.onlineFriendIds.add(uid);
    this._onlineListeners.forEach((fn) => fn(uid));
  }

  _markOffline(uid) {
    if (!this.onlineFriendIds.has(uid)) return;
    this.onlineFriendIds.delete(uid);
    this._offlineListeners.forEach((fn) => fn(uid));
  }
}

if (typeof window !== 'undefined') window.BubblePresence = BubblePresence;
if (typeof module !== 'undefined' && module.exports) module.exports = BubblePresence;

/* ---------------------------------------------------------------------
   接入 index.html 的改动点：

   1. enterApp(acc) 里，紧跟着原有的
        BubbleComm.joinRoom(state.me.userId);
      加：
        const presence = new BubblePresence({ transport: BubbleComm, myUserId: acc.userId });
        presence.onFriendOnline((uid) => { if(!P2P.isChannelOpen(uid)) P2P.connect(uid); });
        presence.onFriendOffline((uid) => { updateP2PBadge(uid); }); // 好友离线了，把在线小圆点熄掉
        presence.announce(state.friends.filter(f=>!f.status||f.status==='accepted').map(f=>f.userId));
        window.Presence = presence;

   2. 删掉 p2pConnectSweep() 的调用和定义（原来在 enterApp 末尾、以及
      setInterval 定时任务里各调用一次）——不再需要定时全量遍历好友建连接。
      "手动点开聊天窗口时兜底建连接"这条规则继续保留：openConvo(id) 里
      对 single 类型会话调用 P2P.connect(c.id) 那一行不用动，这正是文档
      里"无论有没有收到上线事件，用户手动点开聊天窗口都主动触发建连"
      的兜底逻辑，跟事件驱动上线通知并不冲突，两者互补。

   3. 好友列表发生变化时（接受了新的好友请求 acceptFriendRequest、
      或者别处新增了好友）重新调用一次 Presence.announce(...)，
      把新好友也加进通知范围。

   4. setupResyncOnReturn() 里原来 visibilitychange/focus/online 事件
      触发的 P2P.verifyConnectionsOnForeground() 继续保留（那是校验
      "标签页从后台切回来、残留连接是否还活着"，跟这里的"好友上线
      通知"是两回事，不冲突）；可以额外加一行 presence.announce(...)，
      避免网络抖动导致服务端一侧的信箱连接被重建、之前的在线状态
      快照错过。
--------------------------------------------------------------------- */
