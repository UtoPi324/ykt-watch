#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 雨课堂课堂习题监听 + 自动作答。纯标准库。
#   python ykt_watch.py            # 自动答题
#   python ykt_watch.py --detect   # 只检测不提交
#   python ykt_watch.py --check    # 自检：配置 + 连一次 WS，看完就退
import base64, json, os, re, socket, ssl, struct, subprocess, sys, time
import urllib.error, urllib.parse, urllib.request

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HOST = "https://changjiang.yuketang.cn"
WS_HOST = "changjiang.yuketang.cn"
WS_PATH = "/wsapp/"
UA = ("Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/150.0.0.0 Mobile Safari/537.36")

HB_INTERVAL = 45          # 空闲这么久发一次心跳
STALL_TIMEOUT = 100       # 心跳发出后这么久没回包 -> 判定连接已死
STALL_TIMEOUT_FINISHED = 150   # 下课之后放宽

DETECT_ONLY = "--detect" in sys.argv
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
SSL_CTX = ssl.create_default_context()
LOCK_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "watch.lock")

STATE = {"cookie": "", "lesson_id": None, "identity_id": None, "lesson_token": None,
         "msgid": 1, "ws": None, "answer": {},
         "answered": set(), "seen": set(), "pending": [], "timeline": []}
AUTH = {"token": ""}


def say(*a):
    print("[%s]" % time.strftime("%H:%M:%S"), *a, flush=True)


def log(kind, obj):
    try:
        with open(os.path.join(LOG_DIR, "events.jsonl"), "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                                "kind": kind, "data": obj}, ensure_ascii=False) + "\n")
    except Exception:
        pass


# ----------------------------------------------------------------- 网络
def http(method, path, params=None, body=None, timeout=20):
    url = HOST + path + ("?" + urllib.parse.urlencode(params) if params else "")
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Cookie", STATE["cookie"])
    req.add_header("User-Agent", UA)
    req.add_header("Accept", "application/json, text/plain, */*")
    req.add_header("X-Client", "h5")
    req.add_header("Xtbz", "ykt")
    if data is not None:
        req.add_header("Content-Type", "application/json;charset=UTF-8")
    m = re.search(r"csrftoken=([^;]+)", STATE["cookie"])
    if m:
        req.add_header("X-Csrftoken", m.group(1))
    if STATE["lesson_id"]:
        req.add_header("Referer", HOST + "/lesson/student/v3/" + str(STATE["lesson_id"]))
    if AUTH["token"]:
        req.add_header("Authorization", "Bearer " + AUTH["token"])
    try:
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        code, hdrs, raw = resp.status, dict(resp.getheaders()), resp.read()
    except urllib.error.HTTPError as e:
        code, hdrs, raw = e.code, dict(e.headers), e.read()
    except Exception as e:
        say("HTTP 异常:", repr(e))
        log("http_err", {"url": url, "err": repr(e)})
        return None
    if hdrs.get("set-auth") or hdrs.get("Set-Auth"):
        AUTH["token"] = hdrs.get("set-auth") or hdrs.get("Set-Auth")
    text = raw.decode("utf-8", "replace")
    try:
        js = json.loads(text)
    except Exception:
        js = None
    log("http", {"url": url, "status": code,
                 "resp": js if js is not None else text[:2000]})
    return js


def http_ai(body, timeout=60):
    req = urllib.request.Request(CFG["ai_base"].rstrip("/") + "/chat/completions",
                                 data=json.dumps(body).encode(), method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", "Bearer " + CFG["ai_key"])
    try:
        return json.loads(urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX).read())
    except Exception as e:
        say("AI 接口异常:", repr(e))
        return None


def get_image(url, timeout=15):
    if not url:
        return None
    try:
        raw = urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}),
                                     timeout=timeout, context=SSL_CTX).read()
        if len(raw) < 100:
            return None
        mime = "image/png" if url.lower().split("?")[0].endswith(".png") else "image/jpeg"
        return "data:%s;base64,%s" % (mime, base64.b64encode(raw).decode())
    except Exception as e:
        say("图片下载失败:", (url or "")[:80], repr(e)[:80])
        return None


# ----------------------------------------------------------------- WebSocket
class WS:
    def __init__(self):
        self.sock = None
        self.buf = b""

    def connect(self):
        raw = socket.create_connection((WS_HOST, 443), timeout=15)
        try:
            raw.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if hasattr(socket, "TCP_KEEPIDLE"):
                raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
            if hasattr(socket, "TCP_KEEPINTVL"):
                raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
            if hasattr(socket, "TCP_KEEPCNT"):
                raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        except Exception:
            pass
        self.sock = SSL_CTX.wrap_socket(raw, server_hostname=WS_HOST)
        self.sock.settimeout(5)
        self.buf = b""
        key = base64.b64encode(os.urandom(16)).decode()
        req = ("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
               "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n"
               "Origin: %s\r\nUser-Agent: %s\r\nCookie: %s\r\n\r\n"
               % (WS_PATH, WS_HOST, key, HOST, UA, STATE["cookie"]))
        self.sock.sendall(req.encode())
        head = b""
        while b"\r\n\r\n" not in head:
            head += self.sock.recv(4096)
        status = head.split(b"\r\n", 1)[0].decode("latin1")
        if "101" not in status:
            raise RuntimeError("WS 握手失败: " + status)

    def _exact(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("socket closed")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def _frame(self, opcode, payload=b""):
        mask = os.urandom(4)
        n = len(payload)
        head = bytearray([0x80 | opcode])
        if n < 126:
            head.append(0x80 | n)
        elif n < 65536:
            head.append(0x80 | 126)
            head += struct.pack(">H", n)
        else:
            head.append(0x80 | 127)
            head += struct.pack(">Q", n)
        head += mask
        self.sock.sendall(bytes(head) + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))

    def send(self, obj):
        self._frame(1, json.dumps(obj, ensure_ascii=False).encode())

    def recv(self):
        b1, b2 = self._exact(1)[0], self._exact(1)[0]
        op = b1 & 0x0F
        ln = b2 & 0x7F
        if ln == 126:
            ln = struct.unpack(">H", self._exact(2))[0]
        elif ln == 127:
            ln = struct.unpack(">Q", self._exact(8))[0]
        data = self._exact(ln) if ln else b""
        if op == 9:
            self._frame(10, data)
            return self.recv()
        if op == 8:
            raise ConnectionError("server closed")
        if op in (1, 2):
            try:
                return json.loads(data.decode("utf-8"))
            except Exception:
                return {"op": "_raw"}
        return self.recv()

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass


# ----------------------------------------------------------------- 工具
def as_dict(x):
    """服务端的 unlockedproblem 是裸 id 字符串，timeline 里才是对象；统一成 dict。"""
    if isinstance(x, dict):
        return x
    if isinstance(x, (bytes, bytearray)):
        x = x.decode("utf-8", "replace")
    if isinstance(x, str):
        s = x.strip()
        if s[:1] in ("{", "["):
            try:
                j = json.loads(s)
                if isinstance(j, dict):
                    return j
            except Exception:
                pass
        return {"prob": s} if s else {}
    if isinstance(x, (int, float)):
        return {"prob": str(int(x))}
    return {}


def hb_action(now, last_hb, hb_sent_at, over=False):
    """心跳状态机。上一拍必须被回包应答过才发下一拍，否则时间戳被反复覆盖，死链永远判不出来。"""
    limit = STALL_TIMEOUT_FINISHED if over else STALL_TIMEOUT
    if hb_sent_at is not None and now - hb_sent_at > limit:
        return "die", hb_sent_at
    if hb_sent_at is None and now - last_hb > HB_INTERVAL:
        return "beat", now
    return "wait", hb_sent_at


def ws_send(obj):
    try:
        STATE["ws"].send(obj)
    except Exception as e:
        say("ws send 失败:", repr(e))


# ----------------------------------------------------------------- 业务
def checkin():
    js = http("POST", "/api/v3/lesson/checkin", body={"source": 5, "lessonId": STATE["lesson_id"]})
    if not js or js.get("code") != 0:
        say("checkin 失败:", js)
        return False
    d = js.get("data") or {}
    STATE["lesson_token"] = d.get("lessonToken")
    STATE["identity_id"] = d.get("identityId") or d.get("userId")
    say("checkin OK  identityId=%s" % STATE["identity_id"])
    return True


def find_lesson():
    js = http("GET", "/api/v3/classroom/on-lesson-upcoming-exam",
              params={"front_time": int(time.time() * 1000)})
    try:
        for r in js["data"]["onLessonClassrooms"]:
            if r.get("lessonId"):
                return r["lessonId"], r.get("courseName"), r.get("classroomName")
    except Exception:
        pass
    return None, None, None


def fetch_slide(pres, si, sid=None):
    js = http("GET", "/api/v3/lesson/presentation/fetch", params={"presentation_id": pres})
    if not js or js.get("code") != 0:
        say("拉取课件失败")
        return None
    slides = ((js.get("data") or {}).get("slides")) or []
    for s in slides:
        if sid is not None and str(s.get("id")) == str(sid):
            return s
    for s in slides:
        if str(s.get("index")) == str(si):
            return s
    return None


def ask_ai(body, options, ptype, images, timeout=60):
    ops = "\n".join("%s. %s" % (o.get("key"), o.get("value")) for o in options)
    prompt = ("下面是雨课堂选择题（problemType=%s，1=单选 2=多选 3=投票）。\n题干：%s\n选项：\n%s\n\n"
              "只输出正确选项字母；多选按字母顺序连写（如 ABD）。不要任何解释。" % (ptype, body, ops or "(无选项)"))
    content = prompt
    if images:
        content = [{"type": "text", "text": prompt + "\n（配图见图片）"}] + \
                  [{"type": "image_url", "image_url": {"url": im}} for im in images]
    r = http_ai({"model": CFG["ai_model"], "temperature": 0,
                 "messages": [{"role": "user", "content": content}]}, timeout=timeout)
    try:
        txt = r["choices"][0]["message"]["content"].strip()
    except Exception:
        say("AI 返回异常:", json.dumps(r, ensure_ascii=False)[:200] if r else None)
        return None
    valid = {o.get("key") for o in options} if options else set("ABCDEFG")
    got = [c for c in re.findall(r"[A-Z]", txt.upper()) if c in valid]
    if ptype == 1 and got:
        got = got[:1]
    return "".join(sorted(set(got))) or None


def submit(pid, ans, ptype):
    payload = {"problemId": pid, "problemType": ptype,
               "dt": int(time.time() * 1000), "result": sorted(ans)}
    js = http("POST", "/api/v3/lesson/problem/answer", body=payload)
    say("提交[answer] ->", json.dumps(js, ensure_ascii=False)[:200] if js else None)
    if js and js.get("code") == 0:
        return True
    js2 = http("POST", "/api/v3/lesson/problem/retry", body={"problems": [payload]})
    say("提交[retry] ->", json.dumps(js2, ensure_ascii=False)[:200] if js2 else None)
    return bool(js2 and js2.get("code") == 0)


def do_problem(msg):
    msg = as_dict(msg)
    p = as_dict(msg.get("problem")) if "problem" in msg else msg
    if not p.get("prob") and isinstance(msg.get("raw"), str):
        p = {"prob": msg["raw"]}
    pid = p.get("prob")
    if not pid:
        say("unlockproblem 缺 prob:", json.dumps(msg, ensure_ascii=False)[:200])
        return
    pid = str(pid)
    if pid in STATE["answered"]:
        return
    STATE["answered"].add(pid)
    si, pres, sid, limit = p.get("si"), p.get("pres"), p.get("sid"), p.get("limit")
    if pres is None:
        for it in STATE["timeline"]:
            it = as_dict(it)
            if it.get("type") == "problem" and str(it.get("prob")) == pid:
                pres, si = it.get("pres"), si if si is not None else it.get("si")
                sid, limit = sid or it.get("sid"), limit if limit is not None else it.get("limit")
                break
    say("=" * 60)
    say("收到习题 prob=%s slide=%s limit=%s" % (pid, si, limit))
    ws_send({"op": "probleminfo", "lessonid": STATE["lesson_id"], "problemid": pid, "msgid": STATE["msgid"]})
    t0 = time.time()
    slide = fetch_slide(pres, si, sid)
    if not slide or not slide.get("problem"):
        say("课件里没找到这道题 (pres=%s si=%s)" % (pres, si))
        return
    pr = slide["problem"]
    body = pr.get("body") or ""
    options = pr.get("options") or []
    ptype = pr.get("problemType")
    images = [im for im in (get_image(slide.get("cover")),
                            *[get_image((("https:" + s) if s.startswith("//") else s))
                              for s in re.findall(r'<img[^>]+src="([^"]+)"', body)]) if im]
    STATE["answer"][pid] = {"body": body, "options": options, "type": ptype,
                            "limit": limit, "t0": t0, "images": images, "state": "pending"}
    say("题干:", body)
    for o in options:
        say("   %s. %s" % (o.get("key"), o.get("value")))
    log("question", {"prob": pid, "body": body, "type": ptype, "limit": limit})
    if ptype not in (1, 2, 3):
        say("非选择题(type=%s)，请手动作答" % ptype)
        STATE["answer"][pid]["state"] = "skip"
        return
    ans, timeout = ask_ai(body, options, ptype, images), 60
    if not ans:
        ans = ask_ai(body, options, ptype, images, timeout=30)
    if not ans:
        say("AI 未给答案 —— 请手动作答！")
        STATE["answer"][pid]["state"] = "no_ai"
        return
    say("AI 答案:", ans)
    if DETECT_ONLY:
        say("[detect] 未提交")
        return
    if limit is not None and time.time() - t0 > limit - 6:
        say("剩余时间不足，放弃提交")
        STATE["answer"][pid]["state"] = "no_time"
        return
    ok = submit(pid, ans, ptype)
    say("提交成功" if ok else "提交被拒，请手动作答！")
    STATE["answer"][pid]["state"] = "done" if ok else "rejected"


def flush_pending():
    items, STATE["pending"] = STATE["pending"], []
    for m in items:
        try:
            do_problem(m)
        except Exception as e:
            say("补答失败:", repr(e))
            log("pending_err", {"msg": m, "err": repr(e)})


def run_once():
    if not STATE["lesson_token"] and not checkin():
        return
    ws = WS()
    ws.connect()
    STATE["ws"] = ws
    ws.send({"op": "hello", "userid": STATE["identity_id"], "role": "student",
             "auth": STATE["lesson_token"], "lessonid": STATE["lesson_id"]})
    say("WS 已连接，等题中")
    flush_pending()
    last_hb = last_in = time.time()
    hb_sent_at = None
    over = False
    while True:
        try:
            msg = ws.recv()
        except socket.timeout:
            now = time.time()
            act, hb_sent_at = hb_action(now, last_hb, hb_sent_at, over)
            if act == "die":
                raise ConnectionError("心跳 %.0fs 无应答，连接已死" % (now - hb_sent_at))
            if act == "beat":
                ws.send({"op": "fetchtimeline", "lessonid": STATE["lesson_id"], "msgid": STATE["msgid"]})
                STATE["msgid"] += 1
                last_hb = now
            continue
        now = time.time()
        last_in = now
        hb_sent_at = None
        log("ws_in", msg)
        op = msg.get("op")
        if op == "lessonfinished" or msg.get("message") == "lesson finished":
            if not over:
                over = True
                say("本课已结束，继续待机")
        if op == "hello":
            STATE["timeline"] = msg.get("timeline") or []
            say("hello 回包 timeline=%d 条" % len(STATE["timeline"]))
            up = msg.get("unlockedproblem")
            up = up if isinstance(up, list) else ([up] if up else [])
            for it in up:
                it = as_dict(it)
                cand = as_dict(it.get("problem")) if isinstance(it.get("problem"), dict) else it
                pid = str(cand.get("prob") or "")
                if not pid or pid in STATE["seen"]:
                    continue
                STATE["seen"].add(pid)
                STATE["pending"].append(it)
            if STATE["pending"]:
                say("hello 带回 %d 道未答题" % len(STATE["pending"]))
                flush_pending()
        elif op == "unlockproblem":
            try:
                do_problem(msg)
            except Exception as e:
                say("题目处理异常:", repr(e))
                log("problem_err", {"msg": msg, "err": repr(e)})
                STATE["pending"].append(msg)
        elif op == "fetchtimeline":
            for it in (msg.get("timeline") or []):
                it = as_dict(it)
                if it.get("type") != "problem":
                    continue
                pid = str(it.get("prob") or "")
                if pid and pid not in STATE["answered"]:
                    try:
                        do_problem({"problem": it})
                    except Exception as e:
                        log("problem_err", {"msg": it, "err": repr(e)})
        elif op == "extendtime":
            pr = as_dict(msg.get("problem"))
            pid2 = str(pr.get("prob") or "")
            meta = STATE["answer"].get(pid2)
            say("extendtime", pid2, pr.get("limit"))
            if meta and meta.get("state") in ("no_ai", "no_time") and pr.get("limit"):
                ans = ask_ai(meta["body"], meta["options"], meta["type"], meta.get("images") or [])
                if ans and not DETECT_ONLY and submit(pid2, ans, meta["type"]):
                    meta["state"] = "done"
        elif op not in ("notification", "probleminfo", "slidenav"):
            say("op=%s" % op, json.dumps(msg, ensure_ascii=False)[:150])


def check():
    say("版本 0.9.21  脚本 %s" % os.path.abspath(__file__))
    say("cookie %s / ai %s" % ("已设置" if "sessionid=" in STATE["cookie"] else "缺失",
                               CFG["ai_model"] if CFG["ai_key"] else "未配置"))
    if "sessionid=" not in STATE["cookie"]:
        return 2
    if not STATE["lesson_id"]:
        STATE["lesson_id"] = find_lesson()[0]
    if not STATE["lesson_id"]:
        say("当前没有正在上课的课堂")
        return 0
    if not checkin():
        return 3
    ws = WS()
    ws.connect()
    say("WS 握手 101 OK，lessonId=%s" % STATE["lesson_id"])
    ws.send({"op": "hello", "userid": STATE["identity_id"], "role": "student",
             "auth": STATE["lesson_token"], "lessonid": STATE["lesson_id"]})
    t0 = time.time()
    while time.time() - t0 < 10:
        try:
            msg = ws.recv()
        except socket.timeout:
            continue
        if isinstance(msg, dict) and msg.get("op") == "hello":
            say("回包 timeline=%d 条  unlockedproblem=%s"
                % (len(msg.get("timeline") or []), json.dumps(msg.get("unlockedproblem"), ensure_ascii=False)[:120]))
            break
    ws.close()
    say("自检结束")
    return 0


def lock_ok():
    """防止开两个进程同抢一节课，互相把题答成已答。"""
    try:
        if os.path.exists(LOCK_PATH):
            old = open(LOCK_PATH).read().strip()
            out = subprocess.run(["tasklist", "/FI", "PID eq %s" % old, "/FO", "CSV", "/NH"],
                                 capture_output=True, text=True, timeout=10).stdout
            if old in out:
                say("已有 watcher 在跑 (PID=%s)，先停掉它或删掉 %s" % (old, LOCK_PATH))
                return False
        open(LOCK_PATH, "w").write(str(os.getpid()))
    except Exception:
        pass
    return True


def main():
    if not lock_ok():
        return
    say("监听启动 PID=%d 模式=%s" % (os.getpid(), "detect" if DETECT_ONLY else "auto"))
    last_find, short, backoff = 0, 0, 5
    while True:
        t_conn = time.time()
        try:
            if not STATE["lesson_id"] or time.time() - last_find > 120:
                last_find = time.time()
                lid, course, room = find_lesson()
                if lid and lid != STATE["lesson_id"]:
                    say("监听课堂: %s / %s  lessonId=%s" % (course, room, lid))
                    STATE["lesson_id"], STATE["lesson_token"] = lid, None
            if not STATE["lesson_id"]:
                time.sleep(10)
                continue
            run_once()
            backoff = 5
        except KeyboardInterrupt:
            say("手动退出")
            try:
                os.remove(LOCK_PATH)
            except Exception:
                pass
            return
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            log("fatal", {"err": repr(e), "tb": tb})
            say("连接断开(%r)" % (e,))
            say(tb.rstrip().splitlines()[-1])
            short = short + 1 if time.time() - t_conn < 20 else 1
            if short >= 3:
                say("连续短命连接，重新 checkin")
                STATE["lesson_token"], short = None, 0
            backoff = min(60, backoff * 2) if time.time() - t_conn < 20 else 5
        time.sleep(backoff)


CFG = {}
_cfg = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
if os.path.exists(_cfg):
    try:
        CFG = json.load(open(_cfg, encoding="utf-8"))
    except Exception as e:
        print("[!] config.json 读取失败: %r" % (e,))
        print("    用记事本另存为 UTF-8（不要 UTF-8 with BOM），或检查引号/逗号是否完整")
        sys.exit(1)
STATE["cookie"] = (CFG.get("cookie") or "").strip().strip("'\"").strip()
if (CFG.get("lesson_id") or "").strip():
    STATE["lesson_id"] = (CFG.get("lesson_id") or "").strip()
CFG.setdefault("ai_base", "")
CFG.setdefault("ai_key", "")
CFG.setdefault("ai_model", "deepseek-chat")

if __name__ == "__main__":
    if "--check" in sys.argv:
        sys.exit(check())
    if "sessionid=" not in STATE["cookie"]:
        say("config.json 里的 cookie 必须包含 sessionid=")
        say("F12 -> 网络 -> 刷新 -> 任一 changjiang.yuketang.cn 请求 -> 标头 -> 请求标头 -> Cookie 整条复制")
        sys.exit(1)
    if not STATE["cookie"].isascii():
        say("cookie 里还有中文（占位符没换掉？），sessionid 只由字母数字组成，请复制真实的那一串")
        sys.exit(1)
    if not STATE["lesson_id"]:
        STATE["lesson_id"] = find_lesson()[0]
        say("自动发现 lessonId =", STATE["lesson_id"])
    if not CFG["ai_key"]:
        say("未配置 ai_key，只检测题目提示手动作答")
    main()
