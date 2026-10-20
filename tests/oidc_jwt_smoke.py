# -*- coding: utf-8 -*-
"""oidc_jwt_smoke — v0.7.1 OIDC 第二波：RS256/JWKS 本地验签 + 浏览器登录。

拓扑：Python mock IdP（18969，authorize/token/jwks/introspect）
      + 单节点 S（18970，hybrid 模式 + browser_login，mesh 开启）。

覆盖：
  J0  /v1/auth/oidc/status 公开；Web UI 有 SSO 按钮
  J1  login 302 参数（state/nonce/PKCE S256/client_id/redirect_uri/openid）
  J2  完整 Authorization Code + PKCE：ID token 验签 -> session token ->
      任务 200 / 治理面 403 / 配额仅 team-a / WS 可连 / 伪造内部头无效
  J3  logout 立即失效
  J4  state 重放/未知 state/错误 state cookie -> 400；正确 cookie 可完成
  J5  ID token 错 nonce / 错 aud / 错 iss / 过期 / 错签名 -> 400
  J6  browser Admin 全局；Admin+namespace 拒绝
  J7  hybrid：RS256 access token 本地验签（introspection 0 次）、opaque token
      回落 introspection、坏签名/过期 fail-closed、Admin+namespace 拒绝
  J8  JWKS key rotation：未知 kid 触发刷新后接受新 key
  J9  session TTL 到期 401；过期 session WS 401
  J10 纯 JWT 模式（无 introspection）：JWT 200、opaque/过期 401
  J11 进程存活、日志无 panic
"""
import base64
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tests"))
import engine_store_smoke as es  # noqa: E402
import websocket  # noqa: E402

BIN = os.environ.get("AL_BIN", os.path.expanduser("~/agentlink-target/debug/agentlink"))
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/oidc-jwt-sim")
PORT_IDP, PORT_S = 18969, 18970
NODE = "5f0000000001"
ADMIN = "oidc-jwt-admin"
ISS = f"http://127.0.0.1:{PORT_IDP}"
BASIC = "Basic " + base64.b64encode(b"oidc-client:oidc-secret").decode()
EVIDENCE = []
COUNTS = {}
COUNTS_LOCK = threading.Lock()
KEY1 = os.path.join(ROOT, "key1.pem")
KEY2 = os.path.join(ROOT, "key2.pem")
KEYS = {}
ACTIVE = {"kid": "k1"}
ACTIVE_LOCK = threading.Lock()

GOOD = "good"


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:500], flush=True)


def b64u(b):
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def sign_jwt(claims, kid="k1", key=KEY1):
    header = {"alg": "RS256", "typ": "JWT", "kid": kid}
    si = (b64u(json.dumps(header, separators=(",", ":")).encode()) + "." +
          b64u(json.dumps(claims, separators=(",", ":")).encode()))
    sig = subprocess.run(["openssl", "dgst", "-sha256", "-sign", key],
                         input=si.encode(), stdout=subprocess.PIPE,
                         check=True).stdout
    return si + "." + b64u(sig)


def modulus_of(key):
    out = subprocess.check_output(
        ["openssl", "rsa", "-in", key, "-noout", "-modulus"], text=True).strip()
    h = out.split("=", 1)[1]
    if len(h) % 2:
        h = "0" + h
    n = bytes.fromhex(h)
    while len(n) > 1 and n[0] == 0:
        n = n[1:]
    return n


def load_keys():
    for name, path in (("k1", KEY1), ("k2", KEY2)):
        n = modulus_of(path)
        KEYS[name] = {"n": b64u(n), "e": b64u((65537).to_bytes(3, "big"))}


def count_of(token):
    with COUNTS_LOCK:
        return COUNTS.get(token, 0)


def api(port, path, body=None, method=None, token=None, bearer=False,
        extra_headers=None, timeout=10):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    m = method or ("POST" if body is not None else "GET")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization" if bearer else "X-Auth-Token"] = (
            f"Bearer {token}" if bearer else token)
    headers.update(extra_headers or {})
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=m,
                                 data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            try:
                return r.status, json.loads(raw)
            except Exception:
                return r.status, {"raw": raw}
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}
    except Exception as e:
        return 0, {"error": str(e)[:200]}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


OPENER = urllib.request.build_opener(NoRedirect)


def hget(headers, name):
    for k, v in headers.items():
        if k.lower() == name.lower():
            return v
    return ""


def get_no_redirect(url, timeout=10, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with OPENER.open(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read().decode(errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read().decode(errors="replace")


def probe(port):
    import socket
    with socket.socket() as s:
        s.settimeout(0.4)
        return s.connect_ex(("127.0.0.1", port)) == 0


def kill_stale():
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/cmdline", encoding="utf-8", errors="replace") as f:
                cmd = f.read().replace("\0", " ")
        except Exception:
            continue
        if "oidc-jwt-sim" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


class MockIdp(BaseHTTPRequestHandler):
    codes = {}
    codes_lock = threading.Lock()

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _redirect(self, url):
        self.send_response(302)
        self.send_header("Location", url)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        if u.path == "/jwks":
            with ACTIVE_LOCK:
                kid = ACTIVE["kid"]
            k = KEYS[kid]
            return self._json(200, {"keys": [{
                "kty": "RSA", "kid": kid, "use": "sig", "alg": "RS256",
                "n": k["n"], "e": k["e"]}]})
        if u.path == "/authorize":
            q = urllib.parse.parse_qs(u.query, keep_blank_values=True)
            if (q.get("response_type") or [""])[0] != "code" or \
                    (q.get("client_id") or [""])[0] != "oidc-client":
                return self._json(400, {"error": "bad authorize request"})
            redirect_uri = (q.get("redirect_uri") or [""])[0]
            state = (q.get("state") or [""])[0]
            challenge = (q.get("code_challenge") or [""])[0]
            if not redirect_uri or not state or not challenge:
                return self._json(400, {"error": "missing authorize params"})
            code = "code-" + b64u(os.urandom(12))
            with self.codes_lock:
                self.codes[code] = {
                    "challenge": challenge,
                    "nonce": (q.get("nonce") or [""])[0],
                    "redirect_uri": redirect_uri,
                    "role": (q.get("role") or ["ops"])[0],
                    "namespace": (q.get("namespace") or ["team-a"])[0],
                    "bad_nonce": (q.get("bad_nonce") or ["0"])[0] == "1",
                    "bad_aud": (q.get("bad_aud") or ["0"])[0] == "1",
                    "bad_iss": (q.get("bad_iss") or ["0"])[0] == "1",
                    "bad_sig": (q.get("bad_sig") or ["0"])[0] == "1",
                    "expired": (q.get("expired") or ["0"])[0] == "1",
                }
            sep = "&" if "?" in redirect_uri else "?"
            return self._redirect(redirect_uri + sep + urllib.parse.urlencode(
                {"code": code, "state": state}))
        return self._json(404, {"error": "not found"})

    def do_POST(self):
        u = urllib.parse.urlparse(self.path)
        n = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(n).decode()
        form = urllib.parse.parse_qs(raw)
        if u.path == "/introspect":
            if self.headers.get("Authorization") != BASIC:
                return self._json(401, {"error": "bad client"})
            token = (form.get("token") or [""])[0]
            with COUNTS_LOCK:
                COUNTS[token] = COUNTS.get(token, 0) + 1
            table = {
                "opaque-ops": {"active": True, "role": "ops", "namespace": "team-a"},
                "opaque-admin": {"active": True, "role": "admin"},
            }
            return self._json(200, table.get(token, {"active": False}))
        if u.path == "/token":
            if self.headers.get("Authorization") != BASIC:
                return self._json(401, {"error": "bad client"})
            if (form.get("grant_type") or [""])[0] != "authorization_code":
                return self._json(400, {"error": "bad grant"})
            code = (form.get("code") or [""])[0]
            with self.codes_lock:
                c = self.codes.pop(code, None)
            if not c:
                return self._json(400, {"error": "bad code"})
            if (form.get("redirect_uri") or [""])[0] != c["redirect_uri"]:
                return self._json(400, {"error": "bad redirect_uri"})
            verifier = (form.get("code_verifier") or [""])[0]
            challenge = b64u(hashlib.sha256(verifier.encode()).digest())
            if not verifier or challenge != c["challenge"]:
                return self._json(400, {"error": "bad pkce"})
            now = int(time.time())
            claims = {
                "iss": "https://evil.example" if c["bad_iss"] else ISS,
                "aud": "wrong-client" if c["bad_aud"] else "oidc-client",
                "sub": "user-1",
                "iat": now,
                "exp": now - 120 if c["expired"] else now + 300,
                "role": c["role"],
                "nonce": "wrong-nonce" if c["bad_nonce"] else c["nonce"],
            }
            if c["namespace"]:
                claims["namespace"] = c["namespace"]
            with ACTIVE_LOCK:
                active_kid = ACTIVE["kid"]
            active_key = KEY2 if active_kid == "k2" else KEY1
            signed = sign_jwt(claims, kid=active_kid,
                              key=(KEY1 if c["bad_sig"] and active_kid == "k2"
                                   else KEY2 if c["bad_sig"] else active_key))
            return self._json(200, {
                "id_token": signed, "access_token": "opaque-access",
                "token_type": "Bearer", "expires_in": 300})
        return self._json(404, {"error": "not found"})

    def log_message(self, *args):
        pass


class MockThread(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", PORT_IDP), MockIdp)

    def run(self):
        self.httpd.serve_forever(poll_interval=0.1)

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def write_cfg():
    cfg = {
        "v": 1, "node_id": NODE, "name": "oidc-jwt-s", "listen_port": PORT_S,
        "data_dir": os.path.join(ROOT, f"data{PORT_S}"),
        "history": {"enabled": True, "len": 500, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": []},
        "security": {"mode": "strict", "cluster_token": "oidc-jwt-cluster"},
        "local_api": {"host": "127.0.0.1", "token": ADMIN},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 2,
                     "offline_ttl_seconds": 300, "dedup_cache_size": 2048,
                     "persist": False},
        "log": {"level": "info", "format": "text", "file": ""},
        "auth": {
            "tokens": {"static-op": "operator"},
            "oidc": {
                "enabled": True,
                "mode": "hybrid",
                "introspection_url": f"{ISS}/introspect",
                "client_id": "oidc-client",
                "client_secret": "oidc-secret",
                "issuer": ISS,
                "jwks_uri": f"{ISS}/jwks",
                "jwks_cache_seconds": 60,
                "leeway_seconds": 60,
                "role_claim": "role",
                "role_map": {"admin": "admin", "ops": "operator", "aud": "viewer"},
                "cache_seconds": 5,
                "timeout_seconds": 2,
                "namespace_claim": "namespace",
                "browser_login": True,
                "authorization_endpoint": f"{ISS}/authorize",
                "token_endpoint": f"{ISS}/token",
                "redirect_uri": f"http://127.0.0.1:{PORT_S}/v1/auth/oidc/callback",
                "post_login_path": "/web",
                "scopes": "openid profile email",
                "session_seconds": 3,
            },
        },
        "mesh": {"enabled": True, "license_file": "license.toml",
                 "engine": {"max_retries": 0, "backoff_secs": 1, "tick_ms": 200,
                            "max_concurrent_per_node": 4}},
    }
    path = os.path.join(ROOT, "s.json")
    json.dump(cfg, open(path, "w"))
    return path


def write_cfg2():
    """纯 JWT 模式：无 introspection_url/client_secret，opaque token 必须 401。"""
    cfg = {
        "v": 1, "node_id": "5f0000000002", "name": "oidc-jwt-only",
        "listen_port": 18971,
        "data_dir": os.path.join(ROOT, "data18971"),
        "history": {"enabled": True, "len": 200, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": []},
        "security": {"mode": "strict", "cluster_token": "oidc-jwt-cluster"},
        "local_api": {"host": "127.0.0.1", "token": "oidc-jwt-local2"},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 2,
                     "offline_ttl_seconds": 300, "dedup_cache_size": 2048,
                     "persist": False},
        "log": {"level": "info", "format": "text", "file": ""},
        "auth": {
            "oidc": {
                "enabled": True,
                "mode": "jwt",
                "client_id": "oidc-client",
                "issuer": ISS,
                "jwks_uri": f"{ISS}/jwks",
                "jwks_cache_seconds": 60,
                "leeway_seconds": 60,
                "role_claim": "role",
                "role_map": {"admin": "admin", "ops": "operator"},
                "cache_seconds": 5,
                "timeout_seconds": 2,
                "namespace_claim": "namespace",
                "browser_login": False,
            },
        },
        "mesh": {"enabled": True, "license_file": "license.toml",
                 "engine": {"max_retries": 0, "backoff_secs": 1, "tick_ms": 200,
                            "max_concurrent_per_node": 4}},
    }
    path = os.path.join(ROOT, "s2.json")
    json.dump(cfg, open(path, "w"))
    return path


def wait_for(fn, timeout_s=25, step=0.2):
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return fn()


def start_login(extra="", cookie_override=None):
    """走 mock authorize -> node callback；返回 (status, headers, body, login_url, cb_url, cookie)。"""
    st, h, _ = get_no_redirect(f"http://127.0.0.1:{PORT_S}/v1/auth/oidc/login")
    if st != 307:
        return st, h, "", "", "", ""
    login_url = hget(h, "Location")
    cookie = hget(h, "Set-Cookie").split(";", 1)[0]
    auth_url = login_url
    if extra:
        auth_url += ("&" if "?" in auth_url else "?") + extra
    st2, h2, b2 = get_no_redirect(auth_url)
    if st2 != 302:
        return st2, h2, b2, login_url, "", cookie
    cb_url = hget(h2, "Location")
    use_cookie = cookie if cookie_override is None else cookie_override
    st3, h3, b3 = get_no_redirect(
        cb_url, headers={"Cookie": use_cookie} if use_cookie else None)
    return st3, h3, b3, login_url, cb_url, cookie


def session_from_headers(headers):
    loc = hget(headers, "Location")
    if not loc.startswith("/web#token="):
        return None, loc
    return loc.split("#token=", 1)[1], loc


def jwt_token(role="ops", ns="team-a", kid=None, key=KEY1, exp_delta=300,
              sub="jwt-user"):
    kid = kid or (ACTIVE["kid"])
    claims = {
        "iss": ISS, "aud": "oidc-client", "sub": sub,
        "iat": int(time.time()), "exp": int(time.time()) + exp_delta,
        "role": role,
    }
    if ns:
        claims["namespace"] = ns
    return sign_jwt(claims, kid=kid, key=key)


def ws_status(token, client=""):
    if not token:
        return 0
    url = f"ws://127.0.0.1:{PORT_S}/v1/ws?token={urllib.parse.quote(token)}"
    if client:
        url += "&client=" + urllib.parse.quote(client)
    try:
        ws = websocket.create_connection(url, timeout=3, ping_interval=25)
        ws.close()
        return 101
    except websocket.WebSocketBadStatusException as e:
        return e.status_code
    except Exception:
        return 0


def main():
    kill_stale()
    time.sleep(0.4)
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    subprocess.run(["openssl", "genpkey", "-algorithm", "RSA",
                    "-pkeyopt", "rsa_keygen_bits:2048", "-out", KEY1],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["openssl", "genpkey", "-algorithm", "RSA",
                    "-pkeyopt", "rsa_keygen_bits:2048", "-out", KEY2],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    load_keys()
    lic = es.sign_license_text({
        "tier": "starter", "node_quota": 15,
        "issued": "2026-10-01T00:00:00Z",
        "expires": "2036-12-31T23:59:59Z"}, "oidc jwt sim")
    open(os.path.join(ROOT, "license.toml"), "w").write(lic)
    for p in (PORT_IDP, PORT_S):
        assert not probe(p), f"port {p} busy（生产隔离铁律）"

    mock = MockThread()
    mock.start()
    assert wait_for(lambda: probe(PORT_IDP), 10), "mock IdP 未启动"
    proc = None
    proc2 = None
    try:
        cfg = write_cfg()
        proc = subprocess.Popen([BIN, "run", "--config", cfg, "--no-open"],
                                stdout=open(os.path.join(ROOT, "s.log"), "a"),
                                stderr=subprocess.STDOUT)
        assert wait_for(lambda: probe(PORT_S), 25), "S 未启动"

        # J0 status + Web UI -------------------------------------------------
        st_status, status = api(PORT_S, "/v1/auth/oidc/status")
        st_web, web = api(PORT_S, "/v1/tasks")  # placeholder to ensure json helper
        web_status, web_body = get_no_redirect(
            f"http://127.0.0.1:{PORT_S}/web")[0::2]
        record("J0 status 公开；/web 含 SSO 按钮；无 token 401",
               st_status == 200 and status.get("enabled") is True
               and status.get("browser_login") is True
               and web_status == 200 and "sso-btn" in web_body
               and "logout-btn" in web_body
               and "/v1/auth/oidc/login" in web_body and st_web == 401,
               {"status": status, "web": web_status, "tasks_no_token": st_web})

        # J1 login redirect params ------------------------------------------
        st1, h1, _, login_url, _, _ = start_login()
        # start_login already completes callback; do a separate raw login for params
        st_l, h_l, _ = get_no_redirect(
            f"http://127.0.0.1:{PORT_S}/v1/auth/oidc/login")
        q = urllib.parse.parse_qs(urllib.parse.urlparse(hget(h_l, "Location")).query)
        record("J1 login 302 参数：state/nonce/PKCE S256/redirect_uri/openid",
               st_l == 307 and len(q.get("state", [""])[0]) >= 20
               and len(q.get("nonce", [""])[0]) >= 16
               and len(q.get("code_challenge", [""])[0]) == 43
               and q.get("code_challenge_method") == ["S256"]
               and q.get("client_id") == ["oidc-client"]
               and q.get("redirect_uri") == [
                   f"http://127.0.0.1:{PORT_S}/v1/auth/oidc/callback"]
               and "openid" in q.get("scope", [""])[0],
               {"status": st_l, "query": {k: v[0] for k, v in q.items()}})

        # J2 full browser login -> operator session --------------------------
        st2, h2, b2, login_url2, cb_url2, cookie2 = start_login()
        sess, loc = session_from_headers(h2)
        st_tasks, _ = api(PORT_S, "/v1/tasks", token=sess)
        st_govern, _ = api(PORT_S, "/v1/mesh/status", token=sess)
        st_quota, quota = api(PORT_S, "/v1/mesh/quota", token=sess)
        st_spoof, _ = api(PORT_S, "/v1/mesh/status", token=sess,
                          extra_headers={"X-Agentlink-Oidc-Role": "admin"})
        ws_ok = ws_status(sess) if sess else 0
        record("J2 Code+PKCE 成功：session operator；任务 200/治理 403/配额 team-a/WS 101/伪造头无效",
               st2 == 303 and bool(sess) and sess.startswith("al_sess_")
               and loc.startswith("/web#token=")
               and st_tasks == 200 and st_govern == 403
               and st_quota == 200 and quota.get("scope") == "team-a"
               and st_spoof == 403 and ws_ok == 101,
               {"callback": st2, "session_prefix": (sess or "")[:10],
                "location": ("/web#token=<redacted>"
                             if loc.startswith("/web#token=") else loc[:60]),
                "tasks": st_tasks, "govern": st_govern,
                "quota": st_quota, "quota_scope": quota.get("scope"),
                "spoof": st_spoof, "ws": ws_ok})

        # logout immediately (session TTL 3s)
        st_logout, _ = api(PORT_S, "/v1/auth/logout", method="POST", token=sess)
        st_after_logout, _ = api(PORT_S, "/v1/tasks", token=sess)
        record("J3 logout 立即失效（200 -> 401）",
               st_logout == 200 and st_after_logout == 401,
               {"logout": st_logout, "after": st_after_logout})

        # state replay / unknown state ---------------------------------------
        st_replay, _, _ = get_no_redirect(cb_url2, headers={"Cookie": cookie2})
        st_unknown, _, _ = get_no_redirect(
            f"http://127.0.0.1:{PORT_S}/v1/auth/oidc/callback?code=x&state=bogus")
        st_bad_cookie, _, _, _, cb_cookie, cookie_ok = start_login(
            cookie_override="al_oidc_state=wrong")
        st_good_after_bad, _, _ = get_no_redirect(
            cb_cookie, headers={"Cookie": cookie_ok})
        record("J4 state 重放/未知 state/错误 state cookie -> 400；正确 cookie 仍可完成",
               st_replay == 400 and st_unknown == 400
               and st_bad_cookie == 400 and st_good_after_bad == 303,
               {"replay": st_replay, "unknown": st_unknown,
                "bad_cookie": st_bad_cookie, "good_after_bad": st_good_after_bad})

        # bad ID token variants ---------------------------------------------
        variants = {}
        for name, extra in (
                ("nonce", "bad_nonce=1"),
                ("aud", "bad_aud=1"),
                ("iss", "bad_iss=1"),
                ("expired", "expired=1"),
                ("sig", "bad_sig=1")):
            st, h, _, _, _, _ = start_login(extra)
            variants[name] = st
        record("J5 ID token 错 nonce/aud/iss/过期/签名 -> 400",
               all(v == 400 for v in variants.values()), variants)

        # browser Admin / Admin+namespace -----------------------------------
        st_admin, h_admin, _, _, _, _ = start_login("role=admin&namespace=")
        admin_sess, _ = session_from_headers(h_admin)
        st_admin_govern, _ = api(PORT_S, "/v1/mesh/status", token=admin_sess)
        st_admin_ns, _, _, _, _, _ = start_login("role=admin&namespace=team-a")
        record("J6 browser Admin 全局 200；Admin+namespace 拒绝",
               st_admin == 303 and st_admin_govern == 200 and st_admin_ns == 400,
               {"admin_callback": st_admin, "admin_govern": st_admin_govern,
                "admin_ns": st_admin_ns})

        # JWT access tokens (hybrid local verify) ---------------------------
        base = count_of("jwt-local-ops")
        tok_ops = jwt_token("ops", "team-a", sub="jwt-local-ops")
        st_jwt, _ = api(PORT_S, "/v1/tasks", token=tok_ops, bearer=True)
        st_jwt_govern, _ = api(PORT_S, "/v1/mesh/status", token=tok_ops, bearer=True)
        local_calls = count_of("jwt-local-ops") - base
        st_opaque, _ = api(PORT_S, "/v1/tasks", token="opaque-ops", bearer=True)
        tok_bad = jwt_token("ops", "team-a", key=KEY2, sub="jwt-bad-sig")
        st_bad_sig, _ = api(PORT_S, "/v1/tasks", token=tok_bad, bearer=True)
        tok_exp = jwt_token("ops", "team-a", exp_delta=-120, sub="jwt-expired")
        st_exp, _ = api(PORT_S, "/v1/tasks", token=tok_exp, bearer=True)
        tok_admin = jwt_token("admin", None, sub="jwt-admin")
        st_jwt_admin, _ = api(PORT_S, "/v1/mesh/status", token=tok_admin, bearer=True)
        tok_admin_ns = jwt_token("admin", "team-a", sub="jwt-admin-ns")
        st_jwt_admin_ns, _ = api(PORT_S, "/v1/mesh/status", token=tok_admin_ns,
                                 bearer=True)
        record("J7 hybrid JWT：本地验签 0 次自省；opaque 回落；坏签名/过期/Admin+ns 拒绝",
               st_jwt == 200 and st_jwt_govern == 403 and local_calls == 0
               and st_opaque == 200 and st_bad_sig == 401 and st_exp == 401
               and st_jwt_admin == 200 and st_jwt_admin_ns == 401,
               {"jwt": st_jwt, "govern": st_jwt_govern,
                "introspection_calls": local_calls, "opaque": st_opaque,
                "bad_sig": st_bad_sig, "expired": st_exp,
                "admin": st_jwt_admin, "admin_ns": st_jwt_admin_ns})

        # JWKS rotation ------------------------------------------------------
        time.sleep(2.1)  # 让 unknown-kid 刷新节流（2s）过期
        with ACTIVE_LOCK:
            ACTIVE["kid"] = "k2"
        tok_rot = jwt_token("ops", "team-a", kid="k2", key=KEY2, sub="jwt-rotated")
        st_rot, _ = api(PORT_S, "/v1/tasks", token=tok_rot, bearer=True)
        record("J8 JWKS key rotation：未知 kid 触发刷新后接受 k2",
               st_rot == 200, {"status": st_rot})

        # session TTL expiry + expired WS -----------------------------------
        st_exp_login, h_exp, _, _, _, _ = start_login()
        exp_sess, _ = session_from_headers(h_exp)
        st_live, _ = api(PORT_S, "/v1/tasks", token=exp_sess)
        time.sleep(3.3)
        st_dead, _ = api(PORT_S, "/v1/tasks", token=exp_sess)
        ws_dead = ws_status(exp_sess)
        record("J9 session TTL 到期：3s 前 200、3s 后 401；过期 WS 401",
               st_exp_login == 303 and st_live == 200
               and st_dead == 401 and ws_dead == 401,
               {"login": st_exp_login, "live": st_live,
                "after_ttl": st_dead, "ws": ws_dead})

        # J10 纯 JWT 模式（无 introspection 配置） --------------------------
        assert not probe(18971), "port 18971 busy"
        cfg2 = write_cfg2()
        proc2 = subprocess.Popen([BIN, "run", "--config", cfg2, "--no-open"],
                                 stdout=open(os.path.join(ROOT, "s2.log"), "a"),
                                 stderr=subprocess.STDOUT)
        assert wait_for(lambda: probe(18971), 25), "S2 未启动"
        before_opaque = count_of("opaque-admin")
        tok2 = jwt_token("ops", "team-a", kid="k2", key=KEY2, sub="jwt-only-ops")
        st2_jwt, _ = api(18971, "/v1/tasks", token=tok2, bearer=True)
        st2_admin = jwt_token("admin", None, kid="k2", key=KEY2, sub="jwt-only-admin")
        st2_admin_status, _ = api(18971, "/v1/mesh/status", token=st2_admin,
                                  bearer=True)
        st2_opaque, _ = api(18971, "/v1/tasks", token="opaque-admin", bearer=True)
        st2_exp = jwt_token("ops", "team-a", kid="k2", key=KEY2,
                            exp_delta=-120, sub="jwt-only-expired")
        st2_exp_status, _ = api(18971, "/v1/tasks", token=st2_exp, bearer=True)
        after_opaque = count_of("opaque-admin")
        record("J10 mode=jwt（无自省配置）：JWT 200、Admin 200、opaque/过期 401、未打 IdP",
               st2_jwt == 200 and st2_admin_status == 200
               and st2_opaque == 401 and st2_exp_status == 401
               and after_opaque == before_opaque,
               {"jwt": st2_jwt, "admin": st2_admin_status,
                "opaque": st2_opaque, "expired": st2_exp_status,
                "introspection_calls": after_opaque - before_opaque})

        alive = proc.poll() is None and proc2.poll() is None
        log = open(os.path.join(ROOT, "s.log")).read().lower()
        log2 = open(os.path.join(ROOT, "s2.log")).read().lower()
        record("J11 进程存活、日志无 panic",
               alive and "panic" not in log and "panic" not in log2,
               {"alive": alive, "panic_s": "panic" in log,
                "panic_s2": "panic" in log2})

    finally:
        for p in (proc, proc2):
            if p:
                try:
                    p.send_signal(signal.SIGINT)
                    p.wait(timeout=6)
                except Exception:
                    p.kill()
        mock.stop()
        with open(os.path.join(REPO, "tests", "oidc_jwt_smoke.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"oidc_jwt_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
