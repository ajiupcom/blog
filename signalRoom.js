// ============ signalRoom.js (weibo-clone chat) ============
// 一个 Durable Object 实例 == 一个"房间"。跟泡泡IM那套 signalRoom.js是
// 同一个设计思路，搬过来改了绑定名（env.DB 而不是 env.DB_KV）、去掉了
// 泡泡IM特有的"好友请求/群邀请协议消息"语义（这个项目没有群，加好友走
// 现成的 REST /api/friends/request，不需要走信箱），并且把"通知在线/
// 离线"泛化成"通知任意一条消息"——因为这个项目还需要用同一条信箱连接，
// 把已经存进D1的文字消息实时推给正在线的收件人（见下面 chat_message 的
// 说明），不只是好友上下线这一种通知。
//
// 房间有两种用途：
//   1. 信令中转房间：dm_<a>_<b>（WebRTC握手用，图片/文件/语音消息、
//      通话邀请等走这条通道）。这类房间只做一件事——把收到的消息原样
//      广播给房间里除发送者外的所有人，不解密、不关心内容（这个项目
//      当前不加密，见 worker.js 顶部说明——crypto.js/BubbleCrypto 留着
//      没用上，以后要上加密，这里完全不用改）。
//
//   2. 个人信箱房间：room 参数 === uid 参数的连接，是这个用户的"个人
//      信箱"——登录后会一直保持在自己的个人信箱房间里。两个用途：
//        a) 在线状态：个人信箱房间有连接 = 这个人在线，心跳+alarm主动
//           探活（见下方），不是纯粹依赖WebSocket的close事件。
//        b) 实时消息推送：文字/表情消息走 POST /api/messages 存进D1
//           （保证离线也能收到），worker.js 在存库成功后会调用这个
//           Durable Object 的 /internal/notify，把消息内容顺带推给
//           收件人的信箱连接（如果人正在线），前端收到后立即展示、
//           标记已读，不用等下一次轮询/翻历史记录才看到。
//
// 在线状态怎么通知好友（事件驱动，不轮询）——跟泡泡IM完全一样的机制：
//   - 用户 A 连上自己的个人信箱房间后，客户端发一条控制帧
//     {kind:'announce_friends', ids:[...A的好友ID...]}。
//   - 这个 DO 实例（=A的信箱）对每个好友 ID，通过 Durable Object 之间
//     的直接 fetch（不经过公网）调用"那个好友的信箱房间"的
//     /internal/notify，告诉它"A上线了"。
//   - 对方的信箱 DO 如果当前也在线，把 {kind:'friend_online', from:'A'}
//     推给它自己房间里的连接，并且告诉A的信箱"我这边在线"。
//   - A 的信箱汇总所有在线好友，一次性回推 {kind:'friends_online_snapshot', ids:[...]}。
//   - 好友列表只存在这次WebSocket连接的生命周期内，不落盘。
//
// 离线判定：心跳 + Durable Objects 的 Alarm（不用内存 setInterval，那个
// 扛不住DO休眠重启）。客户端每隔一段时间发 {kind:'heartbeat'}；这个DO
// 每分钟醒一次检查，超过 HEARTBEAT_STALE_MS（2分钟）没心跳的连接判定
// 已死——主动标离线、通知好友、关掉僵尸连接，不用干等一个客户端异常
// 退出时可能永远不会来的close事件。

const HEARTBEAT_STALE_MS = 2 * 60 * 1000;
const ALARM_INTERVAL_MS = 60 * 1000;

export class SignalRoom {
  constructor(state, env){
    this.state = state;
    this.env = env; // 需要 env.DB（D1）——跟 Worker 主入口是同一个脚本，自动共享 wrangler.toml 里配置的绑定
  }

  async fetch(request){
    const url = new URL(request.url);

    // 内部调用：来自另一个房间DO实例的"通知"请求（在线/离线/新消息推送），
    // 不是WebSocket升级，直接处理并返回JSON。body本身就是要广播给这个
    // 房间里所有连接的那个帧——调用方（另一个SignalRoom实例，或者
    // worker.js里存完消息之后）想推什么内容，这里原样转发，不关心内容。
    if(url.pathname === '/internal/notify' && request.method === 'POST'){
      return this._handleInternalNotify(request);
    }

    const upgradeHeader = request.headers.get('Upgrade');
    if(!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket'){
      return new Response('expected a websocket upgrade', {status:426});
    }

    const room = url.searchParams.get('room');
    const uid = url.searchParams.get('uid') || crypto.randomUUID();
    const isSelfRoom = room && room === uid; // 约定：room===uid 就是这个人的个人信箱

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.state.acceptWebSocket(server, [uid]);

    if(isSelfRoom){
      this._markOnline(uid).catch(err => console.warn('[SignalRoom] 标记上线失败', err));
      // lastHeartbeatAt 从连接建立这一刻就开始算，不用等第一条心跳/
      // announce_friends到达；后续处理会在这份基础上合并更新，不会丢。
      try{ server.serializeAttachment({ selfUid: uid, friendIds: [], lastHeartbeatAt: Date.now() }); }catch(e){}
      this._ensureAlarmScheduled().catch(err => console.warn('[SignalRoom] 设置探活alarm失败', err));
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message){
    let payload;
    try{ payload = JSON.parse(message); }catch(e){ payload = null; }

    if(payload && payload.kind === 'announce_friends'){
      await this._handleAnnounceFriends(ws, payload);
      return;
    }
    if(payload && payload.kind === 'heartbeat'){
      this._touchHeartbeat(ws);
      return;
    }

    // 普通业务消息（RTC信令、图片/文件/语音消息的P2P载荷等）：原样广播给
    // 房间里除发送者外的所有连接，这个DO不解密也不关心内容。
    const sockets = this.state.getWebSockets();
    for(const peer of sockets){
      if(peer === ws) continue;
      try{ peer.send(message); }catch(e){ /* 节点已离线，忽略 */ }
    }
  }

  async webSocketClose(ws, code, reason, wasClean){
    const attachment = this._safeDeserialize(ws);
    if(!attachment || !attachment.selfUid) return;
    const stillConnected = this.state.getWebSockets().length > 0;
    if(stillConnected) return; // 同一账号多设备/多标签页，还有别的连接在就不算离线
    const friendIds = Array.isArray(attachment.friendIds) ? attachment.friendIds : [];
    await this._handleUserOffline(attachment.selfUid, friendIds);
  }

  async webSocketError(ws, error){
    // 跟close走同一套收尾，webSocketClose通常紧跟着也会触发，这里不用重复处理
  }

  /** 探活alarm，见文件顶部说明 */
  async alarm(){
    const now = Date.now();
    const sockets = this.state.getWebSockets();
    const staleEntries = [];
    let healthySelfUid = null;
    let anySocketAtAll = false;

    for(const ws of sockets){
      anySocketAtAll = true;
      const attachment = this._safeDeserialize(ws);
      if(!attachment || !attachment.selfUid) continue;
      const last = attachment.lastHeartbeatAt || 0;
      if(now - last > HEARTBEAT_STALE_MS) staleEntries.push({ ws, attachment });
      else healthySelfUid = attachment.selfUid;
    }

    for(const { ws } of staleEntries){
      try{ ws.serializeAttachment({}); }catch(e){}
      try{ ws.close(4001, 'heartbeat timeout'); }catch(e){}
    }

    if(staleEntries.length && !healthySelfUid){
      const { attachment } = staleEntries[0];
      const friendIds = Array.isArray(attachment.friendIds) ? attachment.friendIds : [];
      await this._handleUserOffline(attachment.selfUid, friendIds);
    }

    if(anySocketAtAll){
      await this.state.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
    }
  }

  _ensureAlarmScheduled(){
    return this.state.storage.getAlarm().then((existing) => {
      if(existing === null) return this.state.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
    });
  }

  async _handleUserOffline(selfUid, friendIds){
    await this._markOffline(selfUid).catch(err => console.warn('[SignalRoom] 标记离线失败', err));
    await Promise.all(friendIds.map(fid =>
      this._notifyRoom(fid, { kind: 'friend_offline', from: selfUid }).catch(() => {})
    ));
  }

  _touchHeartbeat(ws){
    const prev = this._safeDeserialize(ws);
    const selfUid = (prev && prev.selfUid) || this._tagOf(ws);
    if(!selfUid) return;
    try{ ws.serializeAttachment({ selfUid, friendIds: (prev && prev.friendIds) || [], lastHeartbeatAt: Date.now() }); }
    catch(e){}
  }

  async _handleAnnounceFriends(ws, payload){
    const friendIds = Array.isArray(payload.ids) ? payload.ids.slice(0, 2000) : [];
    const selfUid = payload.selfUid || this._tagOf(ws);
    if(!selfUid) return;

    const prev = this._safeDeserialize(ws);
    try{ ws.serializeAttachment({ selfUid, friendIds, lastHeartbeatAt: (prev && prev.lastHeartbeatAt) || Date.now() }); }catch(e){}

    const onlineFriendIds = [];
    await Promise.all(friendIds.map(async (fid) => {
      try{
        const online = await this._notifyRoom(fid, { kind: 'friend_online', from: selfUid });
        if(online) onlineFriendIds.push(fid);
      }catch(e){}
    }));

    try{ ws.send(JSON.stringify({ kind: 'friends_online_snapshot', ids: onlineFriendIds })); }catch(e){}
  }

  /** 跨DO调用：把 body 原样推给 room 这个人的信箱，返回对方这次是否在线。
      调用方可以是另一个SignalRoom实例（在线/离线通知），也可以是
      worker.js自己（存完一条消息之后推 chat_message 通知）——见
      worker.js 的 notifyUserRoom()。 */
  async _notifyRoom(room, body){
    const id = this.env.ROOMS.idFromName(room);
    const stub = this.env.ROOMS.get(id);
    const resp = await stub.fetch('https://internal/internal/notify', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(body),
    });
    if(!resp.ok) return false;
    const data = await resp.json().catch(() => ({online:false}));
    return !!data.online;
  }

  async _handleInternalNotify(request){
    const body = await request.json().catch(() => null);
    if(!body || !body.kind) return new Response(JSON.stringify({online:false}), {status:400});

    const sockets = this.state.getWebSockets();
    const frame = JSON.stringify(body); // 原样转发整个body，不再收窄成{kind,from}——chat_message这类通知需要带上完整消息内容
    for(const ws of sockets){
      try{ ws.send(frame); }catch(e){}
    }
    return new Response(JSON.stringify({ online: sockets.length > 0 }), {headers:{'Content-Type':'application/json'}});
  }

  _tagOf(ws){
    const tags = this.state.getTags ? this.state.getTags(ws) : null;
    return Array.isArray(tags) && tags.length ? tags[0] : null;
  }

  _safeDeserialize(ws){
    try{ return ws.deserializeAttachment(); }catch(e){ return null; }
  }

  async _markOnline(uid){
    if(!this.env.DB) return;
    await this.env.DB.prepare(
      'UPDATE users SET last_seen_at = ? WHERE id = ?'
    ).bind(new Date().toISOString(), uid).run();
  }

  async _markOffline(uid){
    // 这个项目的"在线"是从 last_seen_at 派生的（now - last_seen_at < 60秒，
    // 见 worker.js），不是一个独立的布尔字段。真正离线这一刻，把
    // last_seen_at 往回拨一天，让派生出来的 isOnline 立即变 false——
    // 不然对于没有实时WebSocket连接、只是刷新页面/查资料页的场景，还要
    // 等 last_seen_at 自然过期（最多60秒）才会显示离线，跟这里已经广播
    // 出去的 friend_offline 实时事件不一致。
    if(!this.env.DB) return;
    await this.env.DB.prepare(
      'UPDATE users SET last_seen_at = ? WHERE id = ?'
    ).bind(new Date(Date.now() - 24*60*60*1000).toISOString(), uid).run();
  }
}
