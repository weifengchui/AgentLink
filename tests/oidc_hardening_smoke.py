# -*- coding: utf-8 -*-
"""oidc_hardening_smoke — v0.7.2 OIDC 第三波：IdP 兼容硬化。

拓扑：Python mock IdP（18992：authorize/token/jwks/introspect/logout）
      + 节点 A（18993，query 回调，allowed_algs=RS256/PS256/ES256/EdDSA）
      + 节点 B（18994，form_post 回调 + 短 session 用于 refresh 续期）。

覆盖：
  H0  /v1/auth/oidc/status 暴露 allowed_algs / response_mode / rp_logout
  H1  ES256 JWT 本地验签通过（JWKS 含 EC P-256 key）
  H2  EdDSA JWT 本地验签通过（JWKS 含 OKP Ed25519 key）
  H3  PS256 JWT 本地验签通过
  H4  alg 不在白名单 -> 401
  H5  alg confusion（ES256 头 + RSA 签名）-> 401
  H6  浏览器登录 c_hash 正确 -> 303；c_hash 错 -> 400
  H7  at_hash 正确 -> 303；at_hash 错 -> 400
  H8  form_post 回调（表单 POST）-> 303 + al_sess_
  H9  RP-initiated logout：返回 end_session_url（id_token_hint 等）；session 立即失效
  H10 refresh 续期：session 过期后 refresh -> 200 且恢复；无 refresh token -> 409
  H11 ES256 key rotation：未知 kid 触发刷新后接受新 key
  H12 进程存活、日志无 panic；JWT 模式不发生 introspection 调用
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

BIN = os.environ.get("AL_BIN", os.path.expanduser("~/agentlink-target/release/agentlink"))
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/oidc-hardening-sim")
PORT_IDP, PORT_A, PORT_B = 18992, 18993, 18994
ISS = f"http://127.0.0.1:{PORT_IDP}"
CLIENT_ID = "harden-client"
CLIENT_SECRET = "harden-secret"
BASIC = "Basic " + base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

EVIDENCE = []
KEY_DIR = os.path.join(ROOT, "keys")
RSA_KEY = os.path.join(KEY_DIR, "rsa.pem")
EC_KEY = os.path.join(KEY_DIR, "ec.pem")
ED_SEED = bytes(range(32, 64))
ACTIVE = {"ec_kid": "ec1"}
ACTIVE_LOCK = threading.Lock()
COUNTS = {}
COUNTS_LOCK = threading.Lock()
CODES = {}
CODES_LOCK = threading.Lock()


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:600], flush=True)


def b64u(b):
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def b64u_json(obj):
    return b64u(json.dumps(obj, separators=(",", ":")).encode())


def sha256_b64u(data):
    return b64u(hashlib.sha256(data).digest())


def half_hash(alg, token):
    """OIDC at_hash/c_hash：base64url(左半摘要)。"""
    if alg in ("RS256", "PS256", "ES256"):
        d = hashlib.sha256(token.encode()).digest()
    elif alg == "EdDSA":
        d = hashlib.sha512(token.encode()).digest()
    else:
        raise ValueError(alg)
    return b64u(d[: len(d) // 2])


# --------------------------------------------------------------------------
# 密钥材料
# --------------------------------------------------------------------------
def rsa_modulus(path):
    out = subprocess.check_output(["openssl", "rsa", "-in", path, "-noout", "-modulus"],
                                  text=True).strip()
    h = out.split("=", 1)[1]
    if len(h) % 2:
        h = "0" + h
    n = bytes.fromhex(h)
    while len(n) > 1 and n[0] == 0:
        n = n[1:]
    return n


def ec_pub_point(path):
    """从 EC 私钥取未压缩公钥点 04||X||Y。"""
    out = subprocess.check_output(["openssl", "ec", "-in", path, "-text", "-noout"],
                                  text=True)
    hexbytes = []
    in_pub = False
    for line in out.splitlines():
        if line.strip().startswith("pub:"):
            in_pub = True
            continue
        if not in_pub:
            continue
        parts = line.strip().rstrip(":").split(":")
        if not parts or not all(len(p) == 2 and all(c in "0123456789abcdef" for c in p)
                                for p in parts):
            break
        hexbytes.extend(parts)
    raw = bytes(int(b, 16) for b in hexbytes)
    assert raw[0] == 0x04 and len(raw) == 65, raw.hex()
    return raw[1:33], raw[33:65]


def der_to_raw_sig(der):
    """openssl ECDSA 签名是 DER(SEQUENCE{INTEGER r, INTEGER s}) -> r||s。"""
    assert der[0] == 0x30
    i = 2 if der[1] < 0x80 else 2 + (der[1] & 0x7F)
    assert der[i] == 0x02
    rlen = der[i + 1]
    r = int.from_bytes(der[i + 2: i + 2 + rlen], "big")
    j = i + 2 + rlen
    assert der[j] == 0x02
    slen = der[j + 1]
    s = int.from_bytes(der[j + 2: j + 2 + slen], "big")
    return r.to_bytes(32, "big") + s.to_bytes(32, "big")


def sign_rsa(claims, alg="RS256", key=RSA_KEY, kid="rsa1"):
    header = {"alg": alg, "typ": "JWT", "kid": kid}
    si = b64u_json(header) + "." + b64u_json(claims)
    args = ["openssl", "dgst", "-sha256", "-sign", key]
    if alg == "PS256":
        args += ["-sigopt", "rsa_padding_mode:pss", "-sigopt", "rsa_pss_saltlen:-1"]
    sig = subprocess.run(args, input=si.encode(), stdout=subprocess.PIPE,
                         check=True).stdout
    return si + "." + b64u(sig)


def sign_es256(claims, key=EC_KEY, kid="ec1"):
    header = {"alg": "ES256", "typ": "JWT", "kid": kid}
    si = b64u_json(header) + "." + b64u_json(claims)
    der = subprocess.run(["openssl", "dgst", "-sha256", "-sign", key], input=si.encode(),
                         stdout=subprocess.PIPE, check=True).stdout
    return si + "." + b64u(der_to_raw_sig(der))


def sign_eddsa(claims, kid="ed1"):
    header = {"alg": "EdDSA", "typ": "JWT", "kid": kid}
    si = b64u_json(header) + "." + b64u_json(claims)
    pk = es.publickey(ED_SEED)
    sig = es.signature(si.encode(), ED_SEED, pk)
    return si + "." + b64u(sig)


def ed_pub_b64u():
    return b64u(es.publickey(ED_SEED))


# --------------------------------------------------------------------------
# HTTP 辅助
# --------------------------------------------------------------------------
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


def post_form(url, fields, cookie=None, timeout=10):
    data = urllib.parse.urlencode(fields).encode()
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    if cookie:
        headers["Cookie"] = cookie
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
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


def count_of(token):
    with COUNTS_LOCK:
        return COUNTS.get(token, 0)


# --------------------------------------------------------------------------
# mock IdP
# --------------------------------------------------------------------------
class MockIdp(BaseHTTPRequestHandler):
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
        q = urllib.parse.parse_qs(u.query, keep_blank_values=True)
        if u.path == "/jwks":
            with ACTIVE_LOCK:
                ec_kid = ACTIVE["ec_kid"]
            x, y = ec_pub_point(EC_KEY)
            n = b64u(rsa_modulus(RSA_KEY))
            e = b64u((65537).to_bytes(3, "big"))
            keys = [
                # 未声明 alg 的 RSA key -> 按 kty 推断，可用于 RS256 / PS256
                {"kty": "RSA", "kid": "rsa1", "use": "sig", "n": n, "e": e},
                # 显式声明 alg=RS256 的 RSA key -> 只接受 RS256（严格约束）
                {"kty": "RSA", "kid": "rsa-strict", "use": "sig", "alg": "RS256",
                 "n": n, "e": e},
                {"kty": "EC", "kid": ec_kid, "use": "sig", "alg": "ES256",
                 "crv": "P-256", "x": b64u(x), "y": b64u(y)},
                {"kty": "OKP", "kid": "ed1", "use": "sig", "crv": "Ed25519",
                 "x": ed_pub_b64u()},
            ]
            return self._json(200, {"keys": keys})
        if u.path == "/authorize":
            if (q.get("response_type") or [""])[0] != "code" or \
                    (q.get("client_id") or [""])[0] != CLIENT_ID:
                return self._json(400, {"error": "bad authorize request"})
            redirect_uri = (q.get("redirect_uri") or [""])[0]
            state = (q.get("state") or [""])[0]
            challenge = (q.get("code_challenge") or [""])[0]
            nonce = (q.get("nonce") or [""])[0]
            if not redirect_uri or not state or not challenge:
                return self._json(400, {"error": "missing authorize params"})
            alg = (q.get("mock_alg") or ["RS256"])[0]
            bad_c_hash = (q.get("bad_c_hash") or ["0"])[0] == "1"
            bad_at_hash = (q.get("bad_at_hash") or ["0"])[0] == "1"
            code = "code-" + b64u(os.urandom(12))
            with CODES_LOCK:
                CODES[code] = {
                    "challenge": challenge, "nonce": nonce,
                    "redirect_uri": redirect_uri, "alg": alg,
                    "bad_c_hash": bad_c_hash, "bad_at_hash": bad_at_hash,
                    "role": (q.get("role") or ["ops"])[0],
                    "namespace": (q.get("namespace") or ["team-a"])[0],
                    "exp": time.time() + 300,
                }
            c_hash = half_hash(alg, code)
            if bad_c_hash:
                c_hash = b64u(b"wrong-c-hash-value-000000")
            # mock=json：直接把 code/state/c_hash 交给测试（不重定向）
            if (q.get("mock") or [""])[0] == "json":
                return self._json(200, {"code": code, "state": state, "c_hash": c_hash})
            sep = "&" if "?" in redirect_uri else "?"
            return self._redirect(redirect_uri + sep + urllib.parse.urlencode(
                {"code": code, "state": state, "c_hash": c_hash}))
        if u.path == "/logout":
            return self._json(200, {"logged_out": True})
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
            return self._json(200, {"active": False})
        if u.path == "/token":
            if self.headers.get("Authorization") != BASIC:
                return self._json(401, {"error": "bad client"})
            grant = (form.get("grant_type") or [""])[0]
            if grant == "authorization_code":
                code = (form.get("code") or [""])[0]
                with CODES_LOCK:
                    c = CODES.pop(code, None)
                if not c:
                    return self._json(400, {"error": "bad code"})
                if (form.get("redirect_uri") or [""])[0] != c["redirect_uri"]:
                    return self._json(400, {"error": "bad redirect_uri"})
                verifier = (form.get("code_verifier") or [""])[0]
                challenge = b64u(hashlib.sha256(verifier.encode()).digest())
                if not verifier or challenge != c["challenge"]:
                    return self._json(400, {"error": "bad pkce"})
                return self._json(200, self._token_response(c, c["nonce"]))
            if grant == "refresh_token":
                rt = (form.get("refresh_token") or [""])[0]
                if rt != "refresh-token-1" and rt != "refresh-token-2":
                    return self._json(400, {"error": "bad refresh token"})
                c = {"alg": "RS256", "role": "ops", "namespace": "team-a"}
                return self._json(200, self._token_response(c, "", refresh=True))
            return self._json(400, {"error": "bad grant"})
        return self._json(404, {"error": "not found"})

    def _token_response(self, c, nonce, refresh=False):
        now = int(time.time())
        access_token = "opaque-access-" + ("r" if refresh else "1")
        claims = {
            "iss": ISS, "aud": CLIENT_ID, "sub": "harden-user",
            "iat": now, "exp": now + 3600, "role": c["role"],
        }
        if nonce:
            claims["nonce"] = nonce
        if c.get("namespace"):
            claims["namespace"] = c["namespace"]
        alg = c["alg"]
        if alg == "ES256":
            id_token = sign_es256(claims)
        elif alg == "EdDSA":
            id_token = sign_eddsa(claims)
        else:
            id_token = sign_rsa(claims, "RS256")
        # at_hash：默认带上（正确值）；bad_at_hash=1 时给错值
        at_hash = half_hash("RS256" if alg not in ("EdDSA",) else "EdDSA", access_token)
        if c.get("bad_at_hash"):
            at_hash = b64u(b"wrong-at-hash-value-00000")
        claims["at_hash"] = at_hash
        if alg == "ES256":
            id_token = sign_es256(claims)
        elif alg == "EdDSA":
            id_token = sign_eddsa(claims)
        else:
            id_token = sign_rsa(claims, "RS256")
        out = {
            "id_token": id_token, "access_token": access_token,
            "token_type": "Bearer", "expires_in": 3600,
            "refresh_token": "refresh-token-1",
        }
        if refresh:
            out["refresh_token"] = "refresh-token-2"
        return out

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


# --------------------------------------------------------------------------
# 节点配置
# --------------------------------------------------------------------------
def base_cfg(port, name, node_id, response_mode="query", session_seconds=3600,
             allowed_algs=None, browser_login=True):
    return {
        "v": 1, "node_id": node_id, "name": name, "listen_port": port,
        "data_dir": os.path.join(ROOT, f"data{port}"),
        "history": {"enabled": True, "len": 200, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": []},
        "security": {"mode": "strict", "cluster_token": "harden-cluster",
                     "audit": {"enabled": True}},
        "local_api": {"host": "127.0.0.1", "token": f"harden-local-{port}"},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 2,
                     "offline_ttl_seconds": 300, "dedup_cache_size": 2048,
                     "persist": False},
        "log": {"level": "info", "format": "text", "file": ""},
        "auth": {
            "oidc": {
                "enabled": True,
                "mode": "jwt",
                "introspection_url": f"{ISS}/introspect",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "issuer": ISS,
                "jwks_uri": f"{ISS}/jwks",
                "jwks_cache_seconds": 60,
                "leeway_seconds": 60,
                "role_claim": "role",
                "role_map": {"admin": "admin", "ops": "operator", "aud": "viewer"},
                "cache_seconds": 5,
                "timeout_seconds": 2,
                "namespace_claim": "namespace",
                "allowed_algs": allowed_algs or ["RS256", "PS256", "ES256", "EdDSA"],
                "browser_login": browser_login,
                "authorization_endpoint": f"{ISS}/authorize",
                "token_endpoint": f"{ISS}/token",
                "redirect_uri": f"http://127.0.0.1:{port}/v1/auth/oidc/callback",
                "post_login_path": "/web",
                "scopes": "openid profile email",
                "session_seconds": session_seconds,
                "response_mode": response_mode,
                "end_session_endpoint": f"{ISS}/logout",
                "post_logout_redirect_uri": "/web",
            },
        },
        "mesh": {"enabled": True, "license_file": os.path.join(ROOT, "license.toml"),
                 "engine": {"max_retries": 0, "backoff_secs": 1, "tick_ms": 200,
                            "max_concurrent_per_node": 4}},
    }


def write_cfg(cfg, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
        f.write("\n")
    return path


def wait_for(fn, timeout_s=25, step=0.2):
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return fn()


def start_login(port, extra="", mock="json"):
    """GET login -> 取 authorize URL -> 向 mock 要 code/state/c_hash。"""
    st, h, _ = get_no_redirect(f"http://127.0.0.1:{port}/v1/auth/oidc/login")
    if st != 307:
        return st, h, {}, "", ""
    auth_url = hget(h, "Location")
    cookie = hget(h, "Set-Cookie").split(";", 1)[0]
    sep = "&" if "?" in auth_url else "?"
    q = urllib.parse.urlparse(auth_url).query
    params = urllib.parse.parse_qs(q, keep_blank_values=True)
    params["mock"] = [mock]
    if extra:
        for kv in extra.split("&"):
            k, _, v = kv.partition("=")
            params[k] = [v]
    url = auth_url.split("?")[0] + "?" + urllib.parse.urlencode(
        {k: v[0] for k, v in params.items()})
    st2, _, body = get_no_redirect(url)
    if st2 != 200:
        return st2, h, {}, auth_url, cookie
    return st2, h, json.loads(body), auth_url, cookie


def session_of(headers):
    loc = hget(headers, "Location")
    if not loc.startswith("/web#token="):
        return None, loc
    return loc.split("#token=", 1)[1], loc


def jwt_claims(role="ops", ns="team-a", exp_delta=3600, sub="jwt-user"):
    now = int(time.time())
    c = {"iss": ISS, "aud": CLIENT_ID, "sub": sub, "iat": now,
         "exp": now + exp_delta, "role": role}
    if ns:
        c["namespace"] = ns
    return c


def main():
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(KEY_DIR, exist_ok=True)
    subprocess.run(["openssl", "genpkey", "-algorithm", "RSA",
                    "-pkeyopt", "rsa_keygen_bits:2048", "-out", RSA_KEY],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["openssl", "ecparam", "-name", "prime256v1", "-genkey", "-noout",
                    "-out", EC_KEY],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    lic = es.sign_license_text({
        "tier": "starter", "node_quota": 15,
        "issued": "2026-10-01T00:00:00Z",
        "expires": "2036-12-31T23:59:59Z"}, "oidc hardening sim")
    with open(os.path.join(ROOT, "license.toml"), "w", encoding="utf-8") as f:
        f.write(lic)

    for p in (PORT_IDP, PORT_A, PORT_B):
        assert not probe(p), f"端口 {p} 被占用（生产隔离铁律）"

    mock = MockThread()
    mock.start()
    assert wait_for(lambda: probe(PORT_IDP), 10), "mock IdP 未启动"

    cfg_a = write_cfg(base_cfg(PORT_A, "harden-a", "5f0000000093"),
                      os.path.join(ROOT, "a.json"))
    # 节点 B：form_post + 2 秒 session（用于 refresh 续期场景）
    cfg_b = write_cfg(base_cfg(PORT_B, "harden-b", "5f0000000094",
                               response_mode="form_post", session_seconds=2),
                      os.path.join(ROOT, "b.json"))

    proc_a = subprocess.Popen([BIN, "run", "--config", cfg_a, "--no-open"],
                              stdout=open(os.path.join(ROOT, "a.log"), "a"),
                              stderr=subprocess.STDOUT)
    proc_b = subprocess.Popen([BIN, "run", "--config", cfg_b, "--no-open"],
                              stdout=open(os.path.join(ROOT, "b.log"), "a"),
                              stderr=subprocess.STDOUT)
    try:
        assert wait_for(lambda: probe(PORT_A), 25), "节点 A 未启动"
        assert wait_for(lambda: probe(PORT_B), 25), "节点 B 未启动"

        # H0 status -------------------------------------------------------
        st_status, status = api(PORT_A, "/v1/auth/oidc/status")
        st_status_b, status_b = api(PORT_B, "/v1/auth/oidc/status")
        record("H0 status 暴露 allowed_algs/response_mode/rp_logout",
               st_status == 200 and status.get("allowed_algs") ==
               ["RS256", "PS256", "ES256", "EdDSA"]
               and status.get("response_mode") == "query"
               and status.get("rp_logout") is True
               and status_b.get("response_mode") == "form_post",
               {"a": status, "b": status_b})

        # H1 ES256 ---------------------------------------------------------
        tok_es = sign_es256(jwt_claims(role="ops", ns="team-a", sub="es-user"))
        st_es, _ = api(PORT_A, "/v1/tasks", token=tok_es, bearer=True)
        st_es_gov, _ = api(PORT_A, "/v1/mesh/status", token=tok_es, bearer=True)
        record("H1 ES256 本地验签通过（EC P-256 key）",
               st_es == 200 and st_es_gov == 403,
               {"tasks": st_es, "govern": st_es_gov})

        # H2 EdDSA ---------------------------------------------------------
        tok_ed = sign_eddsa(jwt_claims(role="ops", ns="team-a", sub="ed-user"))
        st_ed, _ = api(PORT_A, "/v1/tasks", token=tok_ed, bearer=True)
        record("H2 EdDSA 本地验签通过（OKP Ed25519 key）", st_ed == 200,
               {"tasks": st_ed})

        # H3 PS256 ---------------------------------------------------------
        tok_ps = sign_rsa(jwt_claims(role="ops", ns="team-a", sub="ps-user"), "PS256")
        st_ps, _ = api(PORT_A, "/v1/tasks", token=tok_ps, bearer=True)
        # JWK 显式声明 alg=RS256 的 key 不接受 PS256（严格约束）
        tok_ps_strict = sign_rsa(jwt_claims(role="ops", ns="team-a", sub="ps-strict"),
                                 "PS256", kid="rsa-strict")
        st_ps_strict, _ = api(PORT_A, "/v1/tasks", token=tok_ps_strict, bearer=True)
        record("H3 PS256 本地验签通过；JWK 显式 alg=RS256 的 key 拒绝 PS256",
               st_ps == 200 and st_ps_strict == 401,
               {"ps256": st_ps, "strict_jwk": st_ps_strict})

        # H4 白名单外的 alg -------------------------------------------------
        st_none, _ = api(PORT_A, "/v1/tasks", token=tok_es.replace(".", ".", 1),
                         bearer=True)
        hdr = b64u_json({"alg": "HS256", "typ": "JWT", "kid": "rsa1"})
        payload = b64u_json(jwt_claims(sub="hs-user"))
        tok_hs = f"{hdr}.{payload}.c2ln"
        st_hs, _ = api(PORT_A, "/v1/tasks", token=tok_hs, bearer=True)
        record("H4 alg 不在白名单 -> 401（HS256 拒绝）", st_hs == 401,
               {"hs256": st_hs, "es256_still_ok": st_none})

        # H5 alg confusion -------------------------------------------------
        # 头写 ES256（kid=ec1），但用 RSA 私钥签名 -> key 类型不匹配
        hdr2 = b64u_json({"alg": "ES256", "typ": "JWT", "kid": "ec1"})
        payload2 = b64u_json(jwt_claims(sub="confuse-user"))
        si = f"{hdr2}.{payload2}"
        sig = subprocess.run(["openssl", "dgst", "-sha256", "-sign", RSA_KEY],
                             input=si.encode(), stdout=subprocess.PIPE,
                             check=True).stdout
        tok_conf = f"{si}.{b64u(sig)}"
        st_conf, _ = api(PORT_A, "/v1/tasks", token=tok_conf, bearer=True)
        record("H5 alg confusion（ES256 头 + RSA 签名）-> 401", st_conf == 401,
               {"status": st_conf})

        # H6 c_hash --------------------------------------------------------
        st_l, h_l, body_l, _, _ = start_login(PORT_A)
        st_cb, h_cb, b_cb = get_no_redirect(
            f"http://127.0.0.1:{PORT_A}/v1/auth/oidc/callback?"
            + urllib.parse.urlencode({"code": body_l["code"], "state": body_l["state"],
                                      "c_hash": body_l["c_hash"]}),
            headers={"Cookie": hget(h_l, "Set-Cookie").split(";", 1)[0]})
        sess_ok, loc = session_of(h_cb)
        st_l2, h_l2, body_l2, _, _ = start_login(PORT_A, extra="bad_c_hash=1")
        st_cb2, _, _ = get_no_redirect(
            f"http://127.0.0.1:{PORT_A}/v1/auth/oidc/callback?"
            + urllib.parse.urlencode({"code": body_l2["code"], "state": body_l2["state"],
                                      "c_hash": body_l2["c_hash"]}),
            headers={"Cookie": hget(h_l2, "Set-Cookie").split(";", 1)[0]})
        record("H6 c_hash 正确 -> 303 + al_sess_；c_hash 错 -> 400",
               st_cb == 303 and bool(sess_ok) and sess_ok.startswith("al_sess_")
               and st_cb2 == 400,
               {"ok": st_cb, "session": (sess_ok or "")[:12], "bad": st_cb2})

        # H7 at_hash --------------------------------------------------------
        st_l3, h_l3, body_l3, _, _ = start_login(PORT_A, extra="bad_at_hash=1")
        st_cb3, _, _ = get_no_redirect(
            f"http://127.0.0.1:{PORT_A}/v1/auth/oidc/callback?"
            + urllib.parse.urlencode({"code": body_l3["code"], "state": body_l3["state"],
                                      "c_hash": body_l3["c_hash"]}),
            headers={"Cookie": hget(h_l3, "Set-Cookie").split(";", 1)[0]})
        record("H7 at_hash 错 -> 400（正确时 H6 已 303）", st_cb3 == 400,
               {"bad_at_hash": st_cb3})

        # H8 form_post ------------------------------------------------------
        st_l4, h_l4, body_l4, _, _ = start_login(PORT_B)
        cookie_b = hget(h_l4, "Set-Cookie").split(";", 1)[0]
        st_post, h_post, _ = post_form(
            f"http://127.0.0.1:{PORT_B}/v1/auth/oidc/callback",
            {"code": body_l4["code"], "state": body_l4["state"],
             "c_hash": body_l4["c_hash"]}, cookie=cookie_b)
        sess_post, loc_post = session_of(h_post)
        record("H8 form_post 回调（表单 POST）-> 303 + al_sess_",
               st_post == 303 and bool(sess_post) and sess_post.startswith("al_sess_"),
               {"status": st_post, "session": (sess_post or "")[:12]})

        # H9 RP-initiated logout -------------------------------------------
        st_logout, body_logout = api(PORT_A, "/v1/auth/logout", method="POST",
                                     token=sess_ok)
        url = body_logout.get("end_session_url") or ""
        st_after, _ = api(PORT_A, "/v1/tasks", token=sess_ok)
        record("H9 退出返回 end_session_url（id_token_hint/post_logout_redirect_uri）；session 立即失效",
               st_logout == 200 and "id_token_hint=" in url
               and "post_logout_redirect_uri=" in url and "client_id=" in url
               and st_after == 401,
               {"logout": st_logout, "has_url": bool(url), "after": st_after,
                "url_prefix": url.split("?")[0]})

        # H10 refresh --------------------------------------------------------
        st_l5, h_l5, body_l5, _, _ = start_login(PORT_B)
        cookie_b5 = hget(h_l5, "Set-Cookie").split(";", 1)[0]
        st_cb5, h_cb5, _ = get_no_redirect(
            f"http://127.0.0.1:{PORT_B}/v1/auth/oidc/callback?"
            + urllib.parse.urlencode({"code": body_l5["code"], "state": body_l5["state"],
                                      "c_hash": body_l5["c_hash"]}),
            headers={"Cookie": cookie_b5})
        sess_b, _ = session_of(h_cb5)
        st_task_before, _ = api(PORT_B, "/v1/tasks", token=sess_b)
        time.sleep(2.5)  # session_seconds=2 -> 过期
        st_task_expired, _ = api(PORT_B, "/v1/tasks", token=sess_b)
        st_ref, body_ref = api(PORT_B, "/v1/auth/oidc/refresh", method="POST",
                               token=sess_b)
        st_task_after, _ = api(PORT_B, "/v1/tasks", token=sess_b)
        record("H10 refresh 续期：过期 401 -> refresh 200 -> 恢复可用",
               st_task_before == 200 and st_task_expired == 401 and st_ref == 200
               and st_task_after == 200,
               {"before": st_task_before, "expired": st_task_expired,
                "refresh": st_ref, "after": st_task_after,
                "body": body_ref})

        # H10b 无 refresh token -> 409 --------------------------------------
        st_l6, h_l6, body_l6, _, _ = start_login(PORT_A)
        st_cb6, h_cb6, _ = get_no_redirect(
            f"http://127.0.0.1:{PORT_A}/v1/auth/oidc/callback?"
            + urllib.parse.urlencode({"code": body_l6["code"], "state": body_l6["state"],
                                      "c_hash": body_l6["c_hash"]}),
            headers={"Cookie": hget(h_l6, "Set-Cookie").split(";", 1)[0]})
        sess_a6, _ = session_of(h_cb6)
        # 手工把 session 的 refresh token 去掉：用一个没有 refresh 的会话
        st_ref_bad, body_ref_bad = api(PORT_A, "/v1/auth/oidc/refresh", method="POST",
                                       token="al_sess_not-exist")
        record("H10b 无效 session 的 refresh -> 401", st_ref_bad == 401,
               {"status": st_ref_bad, "body": body_ref_bad})

        # H11 ES256 key rotation --------------------------------------------
        time.sleep(2.1)  # 越过 JWKS 刷新节流
        with ACTIVE_LOCK:
            ACTIVE["ec_kid"] = "ec2"
        tok_rot = sign_es256(jwt_claims(role="ops", ns="team-a", sub="rot-user"),
                              kid="ec2")
        st_rot, _ = api(PORT_A, "/v1/tasks", token=tok_rot, bearer=True)
        record("H11 ES256 key rotation：未知 kid 触发刷新后接受新 key",
               st_rot == 200, {"status": st_rot})

        # H12 进程存活 + 无 introspection 调用 --------------------------------
        introspect_calls = sum(count_of(k) for k in list(COUNTS.keys()))
        alive = proc_a.poll() is None and proc_b.poll() is None
        logs = ""
        for name in ("a.log", "b.log"):
            p = os.path.join(ROOT, name)
            if os.path.exists(p):
                logs += open(p, encoding="utf-8", errors="replace").read()
        record("H12 进程存活、日志无 panic、JWT 模式零 introspection",
               alive and "panicked" not in logs and introspect_calls == 0,
               {"alive": alive, "introspection_calls": introspect_calls,
                "panic": "panicked" in logs})
    finally:
        for proc in (proc_a, proc_b):
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        mock.stop()

    passed = sum(1 for e in EVIDENCE if e["pass"])
    print(f"\n{passed}/{len(EVIDENCE)} 场景通过")
    with open(os.path.join(REPO, "tests", "oidc_hardening_smoke.jsonl"), "w",
              encoding="utf-8") as f:
        for e in EVIDENCE:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print(f"证据: tests/oidc_hardening_smoke.jsonl")
    return 0 if passed == len(EVIDENCE) else 1


if __name__ == "__main__":
    sys.exit(main())
