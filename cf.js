/* ============ cf.js ============
   BubbleCF — 用于已部署 Cloudflare Worker 的轻量 REST 客户端。
   优化：请求超时控制、headers 深合并、TURN 错误区分、API_BASE 可配置。
   通过 plain <script src="cf.js"> 加载，向全局暴露 window.BubbleCF。 */
const BubbleCF = (() => {
  let API_BASE = 'https://ws.ajiup.com';
  const DEFAULT_TIMEOUT = 15000;

  /** 切换 API 基地址（开发环境可用），返回旧地址。 */
  function setApiBase(base) {
    const old = API_BASE;
    API_BASE = base.replace(/\/+$/, '');
    return old;
  }

  async function request(path, opts = {}, timeoutMs = DEFAULT_TIMEOUT) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const { headers: customHeaders, ...rest } = opts;
      const res = await fetch(API_BASE + path, {
        ...rest,
        headers: {
          'Content-Type': 'application/json',
          ...customHeaders
        },
        signal: controller.signal
      });
      if (!res.ok) {
        let detail = '';
        try { detail = (await res.json()).error || ''; } catch (_) {}
        const err = new Error(`CF ${opts.method || 'GET'} ${path} -> ${res.status} ${detail}`);
        err.status = res.status;
        throw err;
      }
      return res.json();
    } catch (err) {
      if (err.name === 'AbortError') {
        throw new Error(`CF ${opts.method || 'GET'} ${path} -> timeout (${timeoutMs}ms)`);
      }
      throw err;
    } finally {
      clearTimeout(timer);
    }
  }

  /* 发布公开资料到目录。注册、登录后及资料变更时调用。 */
  async function publishProfile(acc) {
    if (!acc || !acc.userId) return null;
    return request('/publish', {
      method: 'POST',
      body: JSON.stringify({
        userId: acc.userId,
        nickname: acc.nickname,
        avatarEmoji: acc.avatarEmoji,
        color: acc.color,
        publicKeyB64: acc.publicKeyB64
      })
    });
  }

  /* 根据 userId 精确查找，未找到返回 null（不视为错误）。 */
  async function getProfile(userId) {
    if (!userId) return null;
    try {
      return await request('/profile/' + encodeURIComponent(userId));
    } catch (err) {
      if (err.status === 404) return null;
      throw err;
    }
  }

  /* 按 userId（前缀）或 nickname（前缀，不区分大小写）搜索。 */
  async function searchUsers(query) {
    if (!query) return [];
    const data = await request('/search?query=' + encodeURIComponent(query));
    return Array.isArray(data.results) ? data.results : [];
  }

  /* 顺序数字 UserID，由服务端 Durable Object 分配，非尽力而为。 */
  async function allocateUserId() {
    const data = await request('/register/allocate-id', { method: 'POST' });
    return data.userId;
  }

  /* 短期 TURN 凭证。501 = 未配置（正常降级为纯 STUN）；其他错误打日志并返回 null。 */
  async function getTurnCredentials() {
    try {
      const data = await request('/turn/credentials', { method: 'POST' });
      if (!data || !data.iceServers) return null;
      return Array.isArray(data.iceServers) ? data.iceServers : [data.iceServers];
    } catch (err) {
      if (err.status === 501) {
        console.info('[CF] TURN 未配置（501），仅使用 STUN');
      } else {
        console.warn('[CF] TURN 凭证获取失败:', err.message);
      }
      return null;
    }
  }

  return { publishProfile, getProfile, searchUsers, allocateUserId, getTurnCredentials, setApiBase };
})();
