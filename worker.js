/**
 * Weibo-clone sync backend — Cloudflare Worker (v3)
 *
 * v3 adds real-time IM: a WebSocket signaling relay (SignalRoom Durable
 * Object, ported from the 泡泡IM project) for presence + WebRTC P2P
 * handshaking, plus TURN credential issuance. Design (per 2026-09 product
 * decision — see /areas/weibo-clone.md):
 *   - 文字/表情 messages go through this server (D1 `messages` table) —
 *     durable, works even if the recipient is offline. On send, we also
 *     best-effort push it live over the recipient's WebSocket if they're
 *     connected (see notifyRoom); either way it's already safely in D1.
 *     Once the client actually stores a received message locally, it
 *     calls POST /api/messages/read to mark it read server-side.
 *   - 图片/文件/语音消息/语音视频通话 do NOT touch this server at all —
 *     they go peer-to-peer only (WebRTC data channel, TURN as fallback
 *     relay for the connection itself, never for message bodies). This
 *     server has no idea what's inside those; it only ever sees ciphertext-
 *     shaped signaling envelopes it forwards blindly. Both sides must be
 *     online for these to go through — there is no server-side queue for
 *     them, by design (this server can't and shouldn't store bulk binary
 *     chat content — R2 in this project is reserved for post media).
 *   - Encryption is intentionally NOT wired up yet (product call: not a
 *     priority right now) — signaling payloads currently carry plaintext-
 *     shaped envelopes. The client's crypto layer is structured so a real
 *     E2E layer (BubbleCrypto/ECDH, same as 泡泡IM) can be swapped in later
 *     without protocol changes — see NoopCrypto in weibo.html.
 *
 * Bindings expected (see wrangler.toml):
 *   env.DB     - D1 database  (schema.sql)
 *   env.MEDIA  - R2 bucket    (stores images/video/voice files for POSTS only)
 *   env.ROOMS  - Durable Object namespace, bound to SignalRoom (this file)
 *   env.TURN_KEY_ID / env.TURN_KEY_API_TOKEN - optional, for /turn/credentials
 *
 * Auth model: no password login yet... (unchanged, see below)
 * A device calls POST /api/register once, gets back {userId, token}, and
 * stores it locally. Every authenticated request sends
 * `Authorization: Bearer <token>`. Submitting a profile via POST /api/profile
 * is what turns an anonymous device into a "registered" account (sets
 * users.registered_at) — see README "会员体系".
 *
 * Routes:
 *   POST   /api/register              -> {userId, token}
 *   POST   /api/profile        (auth) -> update nickname/bio/avatar_url/cover_url/comment_moderation_mode
 *   POST   /api/heartbeat      (auth) -> updates presence (last_seen_at) — legacy polling fallback, see also /signal
 *   POST   /api/media          (auth) -> upload one file (multipart "file"), returns {url}
 *   POST   /api/posts          (auth) -> upsert a post
 *   DELETE /api/posts/:id      (auth) -> delete a post you own
 *   GET    /api/posts?userId=  -> a user's posts (all fields if it's your own token, else public-only)
 *   GET    /api/square         -> recent public posts across all users, shuffled
 *   POST   /api/likes          (auth) -> toggle like on a post -> {liked, likes}
 *   POST   /api/comments       (auth) -> submit a comment (status decided by the post owner's moderation setting)
 *   GET    /api/comments?postId=      -> approved comments (+ pending ones too, if you're the post owner)
 *   GET    /api/comments/pending-count (auth) -> how many comments on MY posts are awaiting review (for the bell badge)
 *   GET    /api/comments/pending (auth) -> the actual list of pending comments on MY posts, for the moderation panel
 *   POST   /api/comments/:id/moderate (auth) -> post owner approves/rejects a pending comment
 *   POST   /api/friends/request (auth) -> send a friend request -> {ok}
 *   POST   /api/friends/respond (auth) -> accept/reject/block a request -> {ok}
 *   GET    /api/friends        (auth) -> your friends + pending requests
 *   POST   /api/messages       (auth) -> send a private TEXT message (voice/image/file go P2P now, not here)
 *   GET    /api/messages?withUserId=&after=  (auth) -> a message thread (optionally only newer than `after`), marks it read
 *   POST   /api/messages/read  (auth) -> mark specific message ids read (used when a live-pushed message gets stored locally)
 *   GET    /api/messages/unread-count (auth) -> total unread, for the bell badge
 *   GET    /api/messages/unread-by-sender (auth) -> unread count grouped by sender, for contacts-list badges
 *   GET    /media/:key         -> serves an R2 object
 *   GET    /signal?room=&uid=&token=   WebSocket upgrade -> SignalRoom DO (chat/call signaling relay + presence)
 *   POST   /turn/credentials          -> short-lived TURN credentials (501 if not configured, degrades to STUN-only)
 */

export { SignalRoom } from "./signalRoom.js";

function uuid() {
  return crypto.randomUUID();
}
function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    // no-store: 没有这个的话，Cloudflare 的边缘缓存有时会把 GET 接口的返回缓存住，
    // 导致新帖子/新数据要强制刷新才能看到——接口数据必须每次都是新的。
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", ...corsHeaders() },
  });
}
function corsHeaders() {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET,POST,DELETE,OPTIONS",
    "access-control-allow-headers": "authorization,content-type",
  };
}
function err(message, status = 400) {
  return json({ ok: false, error: message }, status);
}

async function getUserByTokenValue(env, token) {
  if (!token) return null;
  const row = await env.DB.prepare("SELECT * FROM users WHERE device_token = ?").bind(token).first();
  return row || null;
}
async function getUserByToken(env, request) {
  const auth = request.headers.get("authorization") || "";
  const m = auth.match(/^Bearer\s+(.+)$/i);
  if (!m) return null;
  return getUserByTokenValue(env, m[1]);
}
// Touch presence whenever an authenticated call comes in — cheap way to keep
// "online" accurate without a dedicated heartbeat call on every action.
async function touchPresence(env, user) {
  await env.DB.prepare("UPDATE users SET last_seen_at = ? WHERE id = ?")
    .bind(new Date().toISOString(), user.id)
    .run();
}
async function areFriends(env, userIdA, userIdB) {
  const row = await env.DB.prepare(
    `SELECT id FROM friends WHERE status='accepted' AND
     ((user_id=? AND friend_id=?) OR (user_id=? AND friend_id=?))`
  )
    .bind(userIdA, userIdB, userIdB, userIdA)
    .first();
  return !!row;
}

// --- Password hashing (PBKDF2-SHA256 via Web Crypto — Workers has no bcrypt) ---
function bytesToHex(bytes) {
  return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
}
function hexToBytes(hex) {
  const arr = new Uint8Array(hex.length / 2);
  for (let i = 0; i < arr.length; i++) arr[i] = parseInt(hex.substr(i * 2, 2), 16);
  return arr;
}
async function hashPassword(password, saltHex) {
  const salt = saltHex ? hexToBytes(saltHex) : crypto.getRandomValues(new Uint8Array(16));
  const keyMaterial = await crypto.subtle.importKey(
    "raw", new TextEncoder().encode(password), "PBKDF2", false, ["deriveBits"]
  );
  const bits = await crypto.subtle.deriveBits(
    { name: "PBKDF2", salt, iterations: 100000, hash: "SHA-256" }, keyMaterial, 256
  );
  return `${bytesToHex(salt)}:${bytesToHex(new Uint8Array(bits))}`;
}
async function verifyPassword(password, stored) {
  const [saltHex] = (stored || "").split(":");
  if (!saltHex) return false;
  return (await hashPassword(password, saltHex)) === stored;
}

// ---------------------------------------------------------------------
// Realtime IM signaling (v3) — WebSocket upgrade -> SignalRoom Durable
// Object (chat/call signaling relay + online/offline presence), plus the
// one helper the REST side needs to push a live notification into
// someone's mailbox room without going through a WebSocket itself.
// See signalRoom.js for what actually happens once a room DO gets a
// message; this file only ever talks to it via env.ROOMS.
// ---------------------------------------------------------------------
const HANDSHAKE_WINDOW_MS = 5 * 60 * 1000;
const HANDSHAKE_LIMIT_PER_WINDOW = 20;
const handshakeCounters = new Map(); // uid -> {windowStart, count} — per-isolate, best-effort (not shared across edge locations), good enough to blunt abusive reconnect loops
function checkHandshakeRateLimit(uid) {
  const now = Date.now();
  const windowStart = Math.floor(now / HANDSHAKE_WINDOW_MS);
  const entry = handshakeCounters.get(uid);
  if (!entry || entry.windowStart !== windowStart) {
    handshakeCounters.set(uid, { windowStart, count: 1 });
    if (handshakeCounters.size > 5000) {
      for (const [k, v] of handshakeCounters) if (v.windowStart !== windowStart) handshakeCounters.delete(k);
    }
    return true;
  }
  if (entry.count >= HANDSHAKE_LIMIT_PER_WINDOW) return false;
  entry.count++;
  return true;
}

async function handleSignal(request, env, url) {
  const room = url.searchParams.get("room");
  const uid = url.searchParams.get("uid");
  const token = url.searchParams.get("token");
  if (!room || !uid) return err("missing room/uid", 400);
  // 浏览器发起WebSocket升级请求没法带 Authorization header，token只能走
  // query string——但同样要验证，不能因为是WS连接就跳过鉴权，否则谁都能
  // 冒充任意uid连上别人的"个人信箱"房间，看到不该看到的在线状态/消息推送。
  const user = await getUserByTokenValue(env, token);
  if (!user || String(user.id) !== String(uid)) return err("unauthorized", 401);
  if (!checkHandshakeRateLimit(uid)) {
    return err("rate limited: too many new connections, please retry shortly", 429);
  }
  const id = env.ROOMS.idFromName(room);
  const stub = env.ROOMS.get(id);
  return stub.fetch(request);
}

/** 把 body 原样推给 room 这个人的信箱连接（如果TA正在线）——调用方是这个
    Worker自己（比如存完一条消息之后想顺手实时推给对方，见 handleMessageSend），
    不是另一个Durable Object实例，但走的是SignalRoom同一个 /internal/notify
    入口，逻辑上完全一致：能推就推，推不到（对方不在线/DO暂时不可用）
    不算错误，调用方原本就不应该依赖这次推送一定成功——真正的持久化
    早就已经完成了。 */
async function notifyUserRoom(env, room, body) {
  const id = env.ROOMS.idFromName(room);
  const stub = env.ROOMS.get(id);
  const resp = await stub.fetch("https://internal/internal/notify", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) return false;
  const data = await resp.json().catch(() => ({ online: false }));
  return !!data.online;
}

// 短期TURN凭证——语音视频通话/P2P直连打不通时的中继兜底。跟 signalRoom.js
// 完全不搭边（那个只管信令怎么送到对方那），这里直接代理 Cloudflare
// Realtime 的凭证签发接口。没配置 TURN_KEY_ID/TURN_KEY_API_TOKEN 就返回
// 501，前端会自动降级成只用免费STUN，不影响其它功能。
async function handleTurnCredentials(request, env) {
  if (!env.TURN_KEY_ID || !env.TURN_KEY_API_TOKEN) {
    return err("TURN 未配置 —— 请设置 TURN_KEY_ID 和 TURN_KEY_API_TOKEN 机密变量", 501);
  }
  const resp = await fetch(`https://rtc.live.cloudflare.com/v1/turn/keys/${env.TURN_KEY_ID}/credentials/generate-ice-servers`, {
    method: "POST",
    headers: { Authorization: `Bearer ${env.TURN_KEY_API_TOKEN}`, "content-type": "application/json" },
    body: JSON.stringify({ ttl: 86400 }),
  });
  const data = await resp.json();
  return json(data, resp.status);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const { pathname } = url;

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders() });
    }

    try {
      if (pathname === "/api/register" && request.method === "POST") return await handleRegister(request, env);
      if (pathname === "/api/login" && request.method === "POST") return await handleLogin(request, env);
      if (pathname === "/api/profile" && request.method === "POST") return await handleProfile(request, env);
      if (pathname === "/api/heartbeat" && request.method === "POST") return await handleHeartbeat(request, env);
      if (pathname === "/api/media" && request.method === "POST") return await handleMediaUpload(request, env);
      if (pathname === "/api/posts" && request.method === "POST") return await handlePostUpsert(request, env);
      if (pathname.startsWith("/api/posts/") && request.method === "DELETE")
        return await handlePostDelete(request, env, pathname.split("/")[3]);
      if (pathname === "/api/posts" && request.method === "GET") return await handlePostsList(request, env, url);
      if (pathname === "/api/square" && request.method === "GET") return await handleSquare(request, env, url);
      if (pathname === "/api/likes" && request.method === "POST") return await handleLikeToggle(request, env);
      if (pathname === "/api/favorites" && request.method === "POST") return await handleFavoriteToggle(request, env);
      if (pathname === "/api/favorites" && request.method === "GET") return await handleFavoritesList(request, env);
      if (pathname === "/api/search" && request.method === "GET") return await handleSearch(request, env, url);
      if (pathname.match(/^\/api\/users\/\d+$/) && request.method === "GET")
        return await handleUserProfile(env, pathname.split("/")[3]);

      if (pathname === "/api/comments" && request.method === "POST") return await handleCommentSubmit(request, env);
      if (pathname === "/api/comments" && request.method === "GET") return await handleCommentsList(request, env, url);
      if (pathname === "/api/comments/pending-count" && request.method === "GET")
        return await handlePendingCommentCount(request, env);
      if (pathname === "/api/comments/pending" && request.method === "GET")
        return await handlePendingCommentList(request, env);
      if (pathname.match(/^\/api\/comments\/[^/]+\/moderate$/) && request.method === "POST")
        return await handleCommentModerate(request, env, pathname.split("/")[3]);

      if (pathname === "/api/friends/request" && request.method === "POST") return await handleFriendRequest(request, env);
      if (pathname === "/api/friends/respond" && request.method === "POST") return await handleFriendRespond(request, env);
      if (pathname === "/api/friends" && request.method === "GET") return await handleFriendsList(request, env);

      if (pathname === "/api/messages" && request.method === "POST") return await handleMessageSend(request, env);
      if (pathname === "/api/messages" && request.method === "GET") return await handleMessageThread(request, env, url);
      if (pathname === "/api/messages/read" && request.method === "POST") return await handleMessageRead(request, env);
      if (pathname === "/api/messages/unread-count" && request.method === "GET")
        return await handleUnreadCount(request, env);
      if (pathname === "/api/messages/unread-by-sender" && request.method === "GET")
        return await handleUnreadBySender(request, env);

      if (pathname === "/signal") return await handleSignal(request, env, url);
      if (pathname === "/turn/credentials" && request.method === "POST")
        return await handleTurnCredentials(request, env);

      if (pathname.startsWith("/media/") && request.method === "GET")
        return await handleMediaServe(env, pathname.slice("/media/".length));
      if (pathname === "/api/media" && request.method === "DELETE") return await handleMediaDelete(request, env);

      return err("not found", 404);
    } catch (e) {
      return err(e.message || "server error", 500);
    }
  },
};

// ---------------------------------------------------------------------
// Registration / login / profile / presence
// ---------------------------------------------------------------------
async function handleRegister(request, env) {
  const body = await safeJson(request);
  const username = (body && body.username || "").trim();
  const password = (body && body.password) || "";
  if (username.length < 3) return err("用户名至少3个字符");
  if (password.length < 6) return err("密码至少6位");
  const existing = await env.DB.prepare("SELECT id FROM users WHERE username = ?").bind(username).first();
  if (existing) return err("用户名已被使用");

  const token = uuid() + uuid();
  const now = new Date().toISOString();
  const passwordHash = await hashPassword(password);
  const result = await env.DB.prepare(
    "INSERT INTO users (username, password_hash, device_token, nickname, bio, created_at) VALUES (?,?,?,?,?,?)"
  )
    .bind(username, passwordHash, token, (body && body.nickname) || username, "", now)
    .run();
  return json({ userId: result.meta.last_row_id, token });
}

async function handleLogin(request, env) {
  const body = await safeJson(request);
  const username = (body && body.username || "").trim();
  const password = (body && body.password) || "";
  const user = await env.DB.prepare("SELECT * FROM users WHERE username = ?").bind(username).first();
  if (!user || !(await verifyPassword(password, user.password_hash))) {
    return err("用户名或密码不正确", 401);
  }
  // Rotate the token on each login (old sessions on other devices simply stop working).
  const token = uuid() + uuid();
  await env.DB.prepare("UPDATE users SET device_token = ? WHERE id = ?").bind(token, user.id).run();
  return json({ userId: user.id, token });
}

async function handleProfile(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const body = await safeJson(request);
  if (!body) return err("bad request");
  const fields = [];
  const values = [];
  for (const key of ["nickname", "bio", "avatar_url", "cover_url"]) {
    if (body[key] !== undefined) {
      fields.push(`${key} = ?`);
      values.push(body[key]);
    }
  }
  if (body.comment_moderation_mode !== undefined) {
    if (!["off", "non_friends", "all"].includes(body.comment_moderation_mode)) {
      return err("invalid comment_moderation_mode");
    }
    fields.push("comment_moderation_mode = ?");
    values.push(body.comment_moderation_mode);
  }
  if (!fields.length) return err("nothing to update");
  // 换头像/背景时，R2 里的旧图片要跟着清掉，否则每换一次就多一张永远用不到的孤儿文件。
  // 要在 UPDATE 之前先把旧的 avatar_url/cover_url 读出来，UPDATE 之后再删——
  // 顺序反了的话旧值就找不到了。只有当新值真的和旧值不同时才删（避免把刚存的新图删掉），
  // R2 删除失败也不影响这次资料更新本身成功与否。
  let oldRow = null;
  if (body.avatar_url !== undefined || body.cover_url !== undefined) {
    oldRow = await env.DB.prepare("SELECT avatar_url, cover_url FROM users WHERE id = ?").bind(user.id).first();
  }
  values.push(user.id);
  await env.DB.prepare(`UPDATE users SET ${fields.join(", ")} WHERE id = ?`).bind(...values).run();
  if (oldRow) {
    const staleKeys = [];
    if (body.avatar_url !== undefined && body.avatar_url !== oldRow.avatar_url) {
      const k = mediaUrlToKey(oldRow.avatar_url);
      if (k) staleKeys.push(k);
    }
    if (body.cover_url !== undefined && body.cover_url !== oldRow.cover_url) {
      const k = mediaUrlToKey(oldRow.cover_url);
      if (k) staleKeys.push(k);
    }
    if (staleKeys.length) {
      await Promise.all(staleKeys.map((k) => env.MEDIA.delete(k).catch(() => {})));
    }
  }
  return json({ ok: true });
}

async function handleHeartbeat(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  await touchPresence(env, user);
  return json({ ok: true });
}

// ---------------------------------------------------------------------
// Media
// ---------------------------------------------------------------------
async function handleMediaUpload(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const form = await request.formData();
  const file = form.get("file");
  if (!file || typeof file.arrayBuffer !== "function") return err("missing file");
  const safeName = (file.name || "upload.bin").replace(/[^a-zA-Z0-9_.-]/g, "_");
  const key = `media/${user.id}/${uuid()}-${safeName}`;
  await env.MEDIA.put(key, await file.arrayBuffer(), {
    httpMetadata: { contentType: file.type || "application/octet-stream" },
  });
  const url = new URL(request.url);
  return json({ url: `${url.origin}/media/${key}`, key });
}

async function handleMediaServe(env, key) {
  const obj = await env.MEDIA.get(key);
  if (!obj) return new Response("not found", { status: 404 });
  const headers = new Headers(corsHeaders());
  obj.writeHttpMetadata(headers);
  headers.set("etag", obj.httpEtag);
  headers.set("cache-control", "public, max-age=31536000, immutable");
  return new Response(obj.body, { headers });
}

// Rollback endpoint: called by the frontend when a post upload (media already
// sent to R2) ultimately fails to save in D1, so the just-uploaded file(s)
// don't sit around as orphaned storage forever. Only deletes files under the
// caller's own user folder (media/{userId}/...) — can't delete anyone else's.
async function handleMediaDelete(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const body = await safeJson(request);
  const key = body && body.key;
  if (!key || !key.startsWith(`media/${user.id}/`)) return err("invalid key");
  await env.MEDIA.delete(key);
  return json({ ok: true });
}

// ---------------------------------------------------------------------
// Posts
// ---------------------------------------------------------------------
async function handlePostUpsert(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  await touchPresence(env, user);
  const body = await safeJson(request);
  if (!body || !body.id) return err("missing post id");
  const now = new Date().toISOString();
  await env.DB.prepare(
    `INSERT INTO posts (id, user_id, text, media_json, voice_url, voice_duration, visibility, pinned_at, created_at, updated_at)
     VALUES (?,?,?,?,?,?,?,?,?,?)
     ON CONFLICT(id) DO UPDATE SET
       text=excluded.text, media_json=excluded.media_json, voice_url=excluded.voice_url,
       voice_duration=excluded.voice_duration, visibility=excluded.visibility,
       pinned_at=excluded.pinned_at, updated_at=excluded.updated_at
     WHERE posts.user_id = ?`
  )
    .bind(
      body.id, user.id, body.text || "", JSON.stringify(body.media || []),
      body.voice ? body.voice.url : null, body.voice ? body.voice.duration : null,
      body.visibility || "private", body.pinnedAt || null,
      body.createdAt || now, body.updatedAt || null, user.id
    )
    .run();
  return json({ ok: true });
}

async function handlePostDelete(request, env, postId) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  if (!postId) return err("missing post id");
  const post = await env.DB.prepare("SELECT * FROM posts WHERE id = ? AND user_id = ?")
    .bind(postId, user.id).first();
  if (!post) return err("not found", 404);

  // Clean up R2 first — a synced post's images/video/voice live there, and
  // deleting the D1 row without them would leave orphaned files forever.
  const keysToDelete = [];
  for (const m of safeParse(post.media_json, [])) {
    const key = mediaUrlToKey(m.url);
    if (key) keysToDelete.push(key);
  }
  const voiceKey = mediaUrlToKey(post.voice_url);
  if (voiceKey) keysToDelete.push(voiceKey);
  await Promise.all(keysToDelete.map((k) => env.MEDIA.delete(k).catch(() => {})));

  // 之前是先 DELETE FROM posts、再删 favorites/comments/likes——但这三张表的
  // post_id 都是 REFERENCES posts(id)，D1 默认开着外键约束，先删父表这一步本身
  // 就会直接报 "FOREIGN KEY constraint failed" 而失败，后面清理子表的语句根本
  // 执行不到。只要这条帖子曾经被人评论/点赞/收藏过就会踩到，这也是"广场上删除
  // 自己的帖子失败"的真正原因（本地帖子那条路径的清理调用又没 await 结果，
  // 所以本地一直没暴露出这个问题）。改成先删引用它的子表记录，最后再删 posts 本身。
  await env.DB.prepare("DELETE FROM favorites WHERE post_id = ?").bind(postId).run();
  await env.DB.prepare("DELETE FROM comments WHERE post_id = ?").bind(postId).run();
  await env.DB.prepare("DELETE FROM likes WHERE post_id = ?").bind(postId).run();
  await env.DB.prepare("DELETE FROM posts WHERE id = ?").bind(postId).run();
  return json({ ok: true });
}
function mediaUrlToKey(url) {
  if (!url) return null;
  const i = url.indexOf("/media/");
  return i === -1 ? null : url.slice(i + "/media/".length);
}

async function handlePostsList(request, env, url) {
  const targetUserId = Number(url.searchParams.get("userId"));
  if (!targetUserId) return err("missing userId");
  const requester = await getUserByToken(env, request);
  const isOwner = requester && requester.id === targetUserId;
  const rows = isOwner
    ? await env.DB.prepare(
        "SELECT * FROM posts WHERE user_id = ? ORDER BY (pinned_at IS NULL), pinned_at DESC, created_at DESC"
      ).bind(targetUserId).all()
    : await env.DB.prepare(
        // "SELECT * FROM posts WHERE user_id = ? AND visibility = 'public' ORDER BY (pinned_at IS NULL), pinned_at DESC, created_at DESC"
        "SELECT posts.*, users.nickname AS author_nickname, users.avatar_url AS author_avatar FROM posts JOIN users ON users.id = posts.user_id WHERE user_id = ? AND visibility = 'public' ORDER BY (pinned_at IS NULL), pinned_at DESC, created_at DESC"
      ).bind(targetUserId).all();
  const posts = (rows.results || []).map(rowToPost);
  await attachCommentPreviews(env, posts);
  return json({ posts });
}

async function handleSquare(request, env, url) {
  const limit = Math.min(50, Number(url.searchParams.get("limit")) || 20);
  // 广场允许匿名浏览，所以这里不强制要求登录；只有登录了才顺便算一下每条帖子的
  // 作者跟"我"的好友状态，好在广场卡片上直接显示"加好友"/"已请求"/自己的帖子不显示。
  const requester = await getUserByToken(env, request);
  const pool = await env.DB.prepare(
    `SELECT posts.*, users.nickname AS author_nickname, users.avatar_url AS author_avatar,
            users.last_seen_at AS author_last_seen
     FROM posts JOIN users ON users.id = posts.user_id
     WHERE posts.visibility = 'public'
     ORDER BY posts.created_at DESC LIMIT 100`
  ).all();
  let friendStatusByUserId = null;
  if (requester) {
    // 一次性把跟"我"有关的好友关系（不管是我发出的还是对方发出的）都查出来，广场上
    // 最多 100 条帖子来自最多 100 个不同作者，这样只用一次额外查询，不用给每条帖子
    // 各发一次 areFriends() 请求（N+1）。
    const rel = await env.DB.prepare(
      `SELECT user_id, friend_id, status FROM friends WHERE user_id = ? OR friend_id = ?`
    ).bind(requester.id, requester.id).all();
    friendStatusByUserId = {};
    for (const r of rel.results || []) {
      const otherId = r.user_id === requester.id ? r.friend_id : r.user_id;
      // accepted 不管谁发起的都算好友；pending 只在"我是发起方"时才该显示成"已请求"——
      // 对方发给我、我还没通过的请求，跟广场上"要不要主动加TA"是两回事，不该混在一起显示。
      if (r.status === "accepted") friendStatusByUserId[otherId] = "accepted";
      else if (r.status === "pending" && r.user_id === requester.id && friendStatusByUserId[otherId] !== "accepted")
        friendStatusByUserId[otherId] = "pending";
    }
  }
  const results = (pool.results || []).map((row) => {
    const post = rowToPost(row);
    // 之前只有 requester 存在时才会设置 authorIsFriend，匿名浏览广场时这个字段
    // 永远是 undefined，前端又要求它 !== undefined 才渲染"加好友"按钮，
    // 结果就是没登录的时候广场帖子左下角完全没有这个按钮。未登录时虽然分不清
    // "是不是我自己发的"，但反正点了也会先跳注册，所以统一当"未加好友"处理即可，
    // 不再留 undefined。
    post.authorIsFriend = requester
      ? (row.user_id === requester.id
          ? null // null = 这是我自己发的帖子，前端据此不显示加好友按钮
          : (friendStatusByUserId[row.user_id] === "accepted" ? true
            : (friendStatusByUserId[row.user_id] === "pending" ? "pending" : false)))
      : false;
    return post;
  });
  shuffle(results);
  const posts = results.slice(0, limit);
  await attachCommentPreviews(env, posts);
  return json({ posts });
}

async function handleLikeToggle(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const body = await safeJson(request);
  if (!body || !body.postId) return err("missing postId");
  const existing = await env.DB.prepare("SELECT id FROM likes WHERE post_id = ? AND user_id = ?")
    .bind(body.postId, user.id).first();
  if (existing) {
    await env.DB.prepare("DELETE FROM likes WHERE id = ?").bind(existing.id).run();
  } else {
    await env.DB.prepare("INSERT INTO likes (id, post_id, user_id, created_at) VALUES (?,?,?,?)")
      .bind(uuid(), body.postId, user.id, new Date().toISOString()).run();
  }
  const count = await env.DB.prepare("SELECT COUNT(*) AS n FROM likes WHERE post_id = ?").bind(body.postId).first();
  return json({ liked: !existing, likes: count.n });
}

async function handleFavoriteToggle(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const body = await safeJson(request);
  if (!body || !body.postId) return err("missing postId");
  const existing = await env.DB.prepare("SELECT id FROM favorites WHERE post_id = ? AND user_id = ?")
    .bind(body.postId, user.id).first();
  if (existing) {
    await env.DB.prepare("DELETE FROM favorites WHERE id = ?").bind(existing.id).run();
  } else {
    await env.DB.prepare("INSERT INTO favorites (id, post_id, user_id, created_at) VALUES (?,?,?,?)")
      .bind(uuid(), body.postId, user.id, new Date().toISOString()).run();
  }
  return json({ favorited: !existing });
}

async function handleFavoritesList(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const rows = await env.DB.prepare(
    `SELECT posts.*, users.nickname AS author_nickname, users.avatar_url AS author_avatar
     FROM favorites JOIN posts ON posts.id = favorites.post_id
     JOIN users ON users.id = posts.user_id
     WHERE favorites.user_id = ? AND posts.visibility = 'public'
     ORDER BY favorites.created_at DESC`
  ).bind(user.id).all();
  const posts = (rows.results || []).map(rowToPost);
  await attachCommentPreviews(env, posts);
  return json({ posts });
}

// Search across public posts' text and users' nicknames — used by the 广场 search box.
async function handleSearch(request, env, url) {
  const q = (url.searchParams.get("q") || "").trim();
  if (!q) return json({ posts: [], users: [] });
  const like = `%${q}%`;
  const posts = await env.DB.prepare(
    `SELECT posts.*, users.nickname AS author_nickname, users.avatar_url AS author_avatar
     FROM posts JOIN users ON users.id = posts.user_id
     WHERE posts.visibility = 'public' AND posts.text LIKE ?
     ORDER BY posts.created_at DESC LIMIT 30`
  ).bind(like).all();
  const users = await env.DB.prepare(
    `SELECT id, nickname, avatar_url, bio FROM users WHERE nickname LIKE ? LIMIT 20`
  ).bind(like).all();
  return json({ posts: (posts.results || []).map(rowToPost), users: users.results || [] });
}

// Public profile card for the "view another blogger's page" feature —
// no auth required (same as browsing the square read-only).
async function handleUserProfile(env, userId) {
  const uid = Number(userId);
  if (!uid) return err("invalid user id");
  const row = await env.DB.prepare(
    "SELECT id, nickname, avatar_url, cover_url, bio, last_seen_at FROM users WHERE id = ?"
  ).bind(uid).first();
  if (!row) return err("not found", 404);
  const ONLINE_WINDOW_MS = 60 * 1000;
  const online = row.last_seen_at ? Date.now() - new Date(row.last_seen_at).getTime() < ONLINE_WINDOW_MS : false;
  return json({
    id: row.id, nickname: row.nickname, avatarUrl: row.avatar_url,
    coverUrl: row.cover_url, bio: row.bio, online,
  });
}

// ---------------------------------------------------------------------
// Comments (with moderation)
// ---------------------------------------------------------------------
async function handleCommentSubmit(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const body = await safeJson(request);
  if (!body || !body.postId) return err("missing postId");
  const post = await env.DB.prepare("SELECT * FROM posts WHERE id = ?").bind(body.postId).first();
  if (!post) return err("post not found", 404);
  const owner = await env.DB.prepare("SELECT * FROM users WHERE id = ?").bind(post.user_id).first();
  const isFriend = owner.id === user.id ? true : await areFriends(env, owner.id, user.id);

  let status = "approved";
  if (owner.comment_moderation_mode === "all") status = "pending";
  else if (owner.comment_moderation_mode === "non_friends" && !isFriend) status = "pending";
  // 'off' -> always approved; post owner commenting on their own post is always approved

  const id = uuid();
  await env.DB.prepare(
    `INSERT INTO comments (id, post_id, user_id, text, voice_url, voice_duration, is_friend_comment, status, created_at)
     VALUES (?,?,?,?,?,?,?,?,?)`
  )
    .bind(id, body.postId, user.id, body.text || "", body.voice ? body.voice.url : null,
      body.voice ? body.voice.duration : null, isFriend ? 1 : 0, status, new Date().toISOString())
    .run();

  // Per the friend-notification idea: an approved friend comment could also drop
  // straight into the chat/message inbox once that UI exists. Not wired up yet —
  // flagging the hook point here rather than guessing at the UX.
  return json({ ok: true, status });
}

async function handleCommentsList(request, env, url) {
  const postId = url.searchParams.get("postId");
  if (!postId) return err("missing postId");
  const post = await env.DB.prepare("SELECT user_id FROM posts WHERE id = ?").bind(postId).first();
  if (!post) return err("post not found", 404);
  const requester = await getUserByToken(env, request);
  const isOwner = requester && requester.id === post.user_id;
  const rows = isOwner
    ? await env.DB.prepare(
        `SELECT comments.*, users.nickname AS author_nickname, users.avatar_url AS author_avatar
         FROM comments JOIN users ON users.id = comments.user_id
         WHERE post_id = ? ORDER BY created_at ASC`
      ).bind(postId).all()
    : await env.DB.prepare(
        `SELECT comments.*, users.nickname AS author_nickname, users.avatar_url AS author_avatar
         FROM comments JOIN users ON users.id = comments.user_id
         WHERE post_id = ? AND status = 'approved' ORDER BY created_at ASC`
      ).bind(postId).all();
  return json({ comments: rows.results || [] });
}

async function handlePendingCommentCount(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const row = await env.DB.prepare(
    `SELECT COUNT(*) AS n FROM comments
     JOIN posts ON posts.id = comments.post_id
     WHERE posts.user_id = ? AND comments.status = 'pending'`
  ).bind(user.id).first();
  return json({ count: row.n });
}

// Lists the actual pending comments (not just the count) for the bell-icon
// moderation panel — post text snippet + commenter nickname included so the
// owner can review without leaving the panel.
async function handlePendingCommentList(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const rows = await env.DB.prepare(
    `SELECT comments.id, comments.text, comments.voice_url, comments.voice_duration, comments.created_at,
            comments.post_id, posts.text AS post_text, users.nickname AS commenter_nickname
     FROM comments
     JOIN posts ON posts.id = comments.post_id
     JOIN users ON users.id = comments.user_id
     WHERE posts.user_id = ? AND comments.status = 'pending'
     ORDER BY comments.created_at DESC`
  ).bind(user.id).all();
  return json({ comments: rows.results || [] });
}

async function handleCommentModerate(request, env, commentId) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const body = await safeJson(request);
  if (!body || !["approved", "rejected"].includes(body.action)) return err("invalid action");
  const comment = await env.DB.prepare(
    `SELECT comments.*, posts.user_id AS post_owner_id FROM comments
     JOIN posts ON posts.id = comments.post_id WHERE comments.id = ?`
  ).bind(commentId).first();
  if (!comment) return err("not found", 404);
  if (comment.post_owner_id !== user.id) return err("forbidden", 403);
  await env.DB.prepare("UPDATE comments SET status = ? WHERE id = ?").bind(body.action, commentId).run();
  return json({ ok: true });
}

// ---------------------------------------------------------------------
// Friends
// ---------------------------------------------------------------------
async function handleFriendRequest(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const body = await safeJson(request);
  const friendId = Number(body && body.friendId);
  if (!friendId) return err("missing friendId");
  if (friendId === user.id) return err("can't friend yourself");
  await env.DB.prepare(
    `INSERT INTO friends (id, user_id, friend_id, status, created_at) VALUES (?,?,?,?,?)
     ON CONFLICT(user_id, friend_id) DO UPDATE SET status='pending'`
  ).bind(uuid(), user.id, friendId, "pending", new Date().toISOString()).run();
  return json({ ok: true });
}

async function handleFriendRespond(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const body = await safeJson(request);
  const friendId = Number(body && body.friendId);
  if (!friendId || !["accepted", "rejected", "blocked"].includes(body.action)) return err("bad request");
  // The request row was created with user_id=friendId, friend_id=me (they requested me).
  await env.DB.prepare("UPDATE friends SET status = ? WHERE user_id = ? AND friend_id = ?")
    .bind(body.action, friendId, user.id).run();
  if (body.action === "accepted") {
    // Mirror row so the relationship is queryable from either side.
    await env.DB.prepare(
      `INSERT INTO friends (id, user_id, friend_id, status, created_at) VALUES (?,?,?,?,?)
       ON CONFLICT(user_id, friend_id) DO UPDATE SET status='accepted'`
    ).bind(uuid(), user.id, friendId, "accepted", new Date().toISOString()).run();
  }
  return json({ ok: true });
}

async function handleFriendsList(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const accepted = await env.DB.prepare(
    `SELECT users.id, users.nickname, users.avatar_url, users.last_seen_at, friends.remark
     FROM friends JOIN users ON users.id = friends.friend_id
     WHERE friends.user_id = ? AND friends.status = 'accepted'`
  ).bind(user.id).all();
  const pendingIncoming = await env.DB.prepare(
    `SELECT users.id, users.nickname, users.avatar_url
     FROM friends JOIN users ON users.id = friends.user_id
     WHERE friends.friend_id = ? AND friends.status = 'pending'`
  ).bind(user.id).all();
  return json({ friends: accepted.results || [], pendingRequests: pendingIncoming.results || [] });
}

// ---------------------------------------------------------------------
// Private messages — 文字/表情走这里持久化到D1（保证对方离线也能收到），
// 发送成功后顺手尝试实时推一次（见 notifyUserRoom）；图片/文件/语音消息/
// 通话完全不经过这个server，走P2P，见文件顶部说明。
// ---------------------------------------------------------------------
async function handleMessageSend(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const body = await safeJson(request);
  const toUserId = Number(body && body.toUserId);
  if (!toUserId) return err("missing toUserId");
  const text = (body && body.text || "").trim();
  if (!text) return err("empty message");
  const id = uuid();
  const createdAt = new Date().toISOString();
  await env.DB.prepare(
    `INSERT INTO messages (id, from_user_id, to_user_id, text, created_at) VALUES (?,?,?,?,?)`
  ).bind(id, user.id, toUserId, text, createdAt).run();
  const message = { id, from_user_id: user.id, to_user_id: toUserId, text, created_at: createdAt, read_at: null };
  // 已经安全存进D1了，这一步只是"锦上添花"：对方如果这会儿正连着WebSocket，
  // 立刻推给TA看到，不用等下一次轮询/翻历史；推送失败（对方不在线，或者
  // Durable Object暂时不可用）完全不影响这条消息本身——反正已经落库了，
  // 对方下次打开会话/拉取未读数的时候一样能看到。
  notifyUserRoom(env, String(toUserId), { kind: "chat_message", message }).catch(() => {});
  return json({ ok: true, id, message });
}

async function handleMessageThread(request, env, url) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const withUserId = Number(url.searchParams.get("withUserId"));
  if (!withUserId) return err("missing withUserId");
  // after: 客户端本地已经缓存了消息（见 weibo.html 的本地聊天记录store），
  // 只需要拉"比我本地最新一条还新"的部分，不用每次都把整个历史都传一遍。
  const after = url.searchParams.get("after");
  const rows = after
    ? await env.DB.prepare(
        `SELECT * FROM messages WHERE
           ((from_user_id = ? AND to_user_id = ?) OR (from_user_id = ? AND to_user_id = ?))
           AND created_at > ?
         ORDER BY created_at ASC`
      ).bind(user.id, withUserId, withUserId, user.id, after).all()
    : await env.DB.prepare(
        `SELECT * FROM messages WHERE
           (from_user_id = ? AND to_user_id = ?) OR (from_user_id = ? AND to_user_id = ?)
         ORDER BY created_at ASC`
      ).bind(user.id, withUserId, withUserId, user.id).all();
  // Reading the thread marks their messages to me as read (drives the bell badge).
  await env.DB.prepare(
    "UPDATE messages SET read_at = ? WHERE from_user_id = ? AND to_user_id = ? AND read_at IS NULL"
  ).bind(new Date().toISOString(), withUserId, user.id).run();
  return json({ messages: rows.results || [] });
}

// 消息通过 chat_message 实时推送直接到达、已经被客户端存进本地聊天记录的
// 情况下，不会再走 GET thread（那个接口本身也会顺带标已读，但客户端这时候
// 可能压根不会调用它——聊天窗口开着的时候消息就直接实时展示了，不需要再
// 拉一次thread）。这个接口就是补上这种情况下"已读"要单独告诉服务器一声。
async function handleMessageRead(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const body = await safeJson(request);
  const ids = Array.isArray(body && body.ids) ? body.ids.slice(0, 200) : [];
  if (!ids.length) return json({ ok: true });
  const placeholders = ids.map(() => "?").join(",");
  // to_user_id=user.id 这个限制很重要——不能让人通过传别人的消息id，
  // 把不属于自己收件箱的消息也标成已读。
  await env.DB.prepare(
    `UPDATE messages SET read_at = ? WHERE to_user_id = ? AND read_at IS NULL AND id IN (${placeholders})`
  ).bind(new Date().toISOString(), user.id, ...ids).run();
  return json({ ok: true });
}

async function handleUnreadCount(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const row = await env.DB.prepare(
    "SELECT COUNT(*) AS n FROM messages WHERE to_user_id = ? AND read_at IS NULL"
  ).bind(user.id).first();
  return json({ count: row.n });
}

// 通讯录列表要在每个好友旁边显示"未读数"角标，不能只靠总未读数——需要按发信人分组。
async function handleUnreadBySender(request, env) {
  const user = await getUserByToken(env, request);
  if (!user) return err("unauthorized", 401);
  const rows = await env.DB.prepare(
    "SELECT from_user_id, COUNT(*) AS n FROM messages WHERE to_user_id = ? AND read_at IS NULL GROUP BY from_user_id"
  ).bind(user.id).all();
  const unreadBySender = {};
  for (const r of rows.results || []) unreadBySender[r.from_user_id] = r.n;
  return json({ unreadBySender });
}

// ---------------------------------------------------------------------
function rowToPost(row) {
  const ONLINE_WINDOW_MS = 60 * 1000;
  const online = row.author_last_seen
    ? Date.now() - new Date(row.author_last_seen).getTime() < ONLINE_WINDOW_MS
    : false;
  return {
    id: row.id,
    userId: row.user_id,
    text: row.text,
    media: safeParse(row.media_json, []),
    voice: row.voice_url ? { url: row.voice_url, duration: row.voice_duration } : null,
    visibility: row.visibility,
    pinnedAt: row.pinned_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    authorNickname: row.author_nickname,
    authorAvatar: row.author_avatar,
    authorOnline: row.author_last_seen !== undefined ? online : undefined,
    // authorIsFriend 由调用方（目前只有 handleSquare）按需补充，其它接口不设置这个字段
  };
}
function safeParse(s, fallback) {
  try { return JSON.parse(s); } catch { return fallback; }
}
// 给一批帖子批量附上"评论数 + 最新3条已通过评论"，用于广场/他人主页/收藏列表卡片上
// 显示"💬 评论 N"和帖子下方的评论预览——用一次 GROUP BY 查数量、一次窗口函数查
// 每条帖子最新3条评论，而不是每条帖子各发一次请求（帖子多的时候会是很重的 N+1）。
async function attachCommentPreviews(env, posts) {
  if (!posts.length) return posts;
  const ids = posts.map((p) => p.id);
  const placeholders = ids.map(() => "?").join(",");
  const countRows = await env.DB.prepare(
    `SELECT post_id, COUNT(*) AS n FROM comments WHERE status = 'approved' AND post_id IN (${placeholders}) GROUP BY post_id`
  ).bind(...ids).all();
  const countByPost = {};
  for (const r of countRows.results || []) countByPost[r.post_id] = r.n;
  const topRows = await env.DB.prepare(
    `SELECT * FROM (
       SELECT comments.id, comments.post_id, comments.text, comments.voice_url, comments.voice_duration,
              comments.created_at, users.nickname AS author_nickname,
              ROW_NUMBER() OVER (PARTITION BY comments.post_id ORDER BY comments.created_at DESC) AS rn
       FROM comments JOIN users ON users.id = comments.user_id
       WHERE comments.status = 'approved' AND comments.post_id IN (${placeholders})
     ) WHERE rn <= 3`
  ).bind(...ids).all();
  const topByPost = {};
  for (const r of topRows.results || []) {
    (topByPost[r.post_id] = topByPost[r.post_id] || []).push({
      id: r.id, text: r.text, voiceUrl: r.voice_url, voiceDuration: r.voice_duration,
      authorNickname: r.author_nickname, createdAt: r.created_at,
    });
  }
  for (const p of posts) {
    p.commentCount = countByPost[p.id] || 0;
    p.topComments = (topByPost[p.id] || []).reverse(); // 展示时按时间正序（旧→新），跟评论区一致
  }
  return posts;
}
async function safeJson(request) {
  try { return await request.json(); } catch { return null; }
}
function shuffle(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
}
