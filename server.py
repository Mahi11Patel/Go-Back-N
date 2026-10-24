import json
import random
import socket
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

HOST = "127.0.0.1"
WEB_PORT = 8000
TCP_PORT = 9001

state_lock = threading.Lock()
state = {}
sim_thread = None
stop_event = threading.Event()
pause_event = threading.Event()
pause_event.set()

sender_sock = None
receiver_ready = threading.Event()
ack_buffer = []
ack_lock = threading.Lock()


def log(message, kind="info"):
    with state_lock:
        state["logs"].append({"message": message, "kind": kind, "time": time.strftime("%H:%M:%S")})
        state["logs"] = state["logs"][-100:]


def generate_loss_frames(total, window):
    """Automatically choose one frame to lose on its first transmission.

    The chosen frame is not shown to the user beforehand. When that frame is
    actually transmitted, the log reports only ``Frame X LOST``. On a Go-Back-N
    timeout, retransmissions are delivered normally.
    """
    if total <= 0:
        return []
    return [random.randrange(total)]


def reset_state():
    global state
    window = 4
    total = 10
    state = {
        "running": False,
        "paused": False,
        "complete": False,
        "window": window,
        "total": total,
        "loss_frames": generate_loss_frames(total, window),
        "timeout": 2200,
        "base": 0,
        "next_seq": 0,
        "expected": 0,
        "last_ack": None,
        "retransmissions": 0,
        "sender": [{"id": i, "status": "waiting"} for i in range(10)],
        "receiver": [{"id": i, "status": "waiting"} for i in range(10)],
        "logs": [],
        "phase": "Ready"
    }

reset_state()


def set_frame(side, idx, status):
    with state_lock:
        state[side][idx]["status"] = status


def wait_if_paused():
    while not pause_event.is_set() and not stop_event.is_set():
        time.sleep(0.05)
    return not stop_event.is_set()


def send_json(sock, obj):
    sock.sendall((json.dumps(obj) + "\n").encode())


def receiver_server():
    global sender_sock
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, TCP_PORT))
    srv.listen(1)
    while True:
        try:
            conn, _ = srv.accept()
            sender_sock = conn
            receiver_ready.set()
            log("TCP connection established: Sender → Receiver.", "ok")
            buffer = b""
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    if not line:
                        continue
                    msg = json.loads(line.decode())
                    if msg.get("type") != "frame":
                        continue
                    seq = msg["seq"]
                    # Receiver implements the GBN rule from the supplied slides:
                    # accept only the frame that is currently expected.
                    with state_lock:
                        expected = state["expected"]
                    time.sleep(0.35)
                    if seq == expected:
                        set_frame("receiver", seq, "received")
                        with state_lock:
                            state["expected"] += 1
                        log(f"Receiver received Frame {seq} in order.", "ok")
                        send_json(conn, {"type": "ack", "ack": seq})
                        log(f"Receiver sent cumulative ACK {seq}.", "ok")
                    else:
                        set_frame("receiver", seq, "discarded")
                        ack = expected - 1
                        send_json(conn, {"type": "ack", "ack": ack})
                        log(f"Receiver discarded Frame {seq} (expected Frame {expected}).", "bad")
                        log(f"Receiver sent duplicate cumulative ACK {ack}.", "warn")
        except Exception:
            time.sleep(0.2)


def wait_for_ack(sock, expected_ack, timeout):
    """Wait for an ACK >= expected_ack while preserving ACKs that arrive early."""
    deadline = time.time() + timeout / 1000.0
    while time.time() < deadline:
        with ack_lock:
            usable = [a for a in ack_buffer if a >= expected_ack]
            if usable:
                highest = max(usable)
                ack_buffer[:] = [a for a in ack_buffer if a > highest]
                return highest
        remaining = max(0.05, deadline - time.time())
        sock.settimeout(remaining)
        try:
            chunk = sock.recv(4096)
            if not chunk:
                return None
            lines = chunk.decode().splitlines()
            for line in lines:
                if not line:
                    continue
                msg = json.loads(line)
                if msg.get("type") == "ack":
                    with ack_lock:
                        ack_buffer.append(msg["ack"])
        except socket.timeout:
            return None
    return None


def transmit_frame(sock, seq, retransmission=False):
    if not wait_if_paused():
        return False
    set_frame("sender", seq, "retransmitting" if retransmission else "sending")
    if retransmission:
        with state_lock:
            state["retransmissions"] += 1
        log(f"Sender retransmitting Frame {seq}.", "warn")
    else:
        log(f"Sender sent Frame {seq}.")

    # Automatic channel loss: the simulator silently chooses one frame at
    # configuration/reset time. The loss is revealed only when that frame is
    # actually transmitted. Retransmissions are delivered normally.
    with state_lock:
        loss_frames = set(state["loss_frames"])
    if seq in loss_frames and not retransmission:
        set_frame("sender", seq, "lost")
        log(f"Frame {seq} LOST", "bad")
        return True

    send_json(sock, {"type": "frame", "seq": seq})
    return True


def simulation():
    global sender_sock
    with state_lock:
        total = state["total"]
        window = state["window"]
        timeout = state["timeout"]

    # Create the actual TCP sender connection.
    for _ in range(50):
        if stop_event.is_set():
            return
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((HOST, TCP_PORT))
            sender_sock = sock
            break
        except OSError:
            time.sleep(0.1)
    else:
        log("Could not establish TCP connection.", "bad")
        return

    global ack_buffer
    with ack_lock:
        ack_buffer.clear()
    base = 0
    next_seq = 0

    with state_lock:
        state["phase"] = "Sending initial window"

    # Send the first complete window slowly: 0,1,2,3.
    while next_seq < min(base + window, total):
        if not wait_if_paused(): return
        transmit_frame(sock, next_seq)
        next_seq += 1
        with state_lock: state["next_seq"] = next_seq
        time.sleep(0.85)

    # Process ACKs while allowing the sender window to advance.
    # We use one oldest-frame timer, matching the Go-Back-N idea.
    while base < total and not stop_event.is_set():
        if not wait_if_paused(): return
        with state_lock:
            state["base"] = base
            state["next_seq"] = next_seq
            state["phase"] = f"Waiting for ACK {base}"

        ack = wait_for_ack(sock, base, timeout)
        if ack is None:
            # Timeout: retransmit all outstanding frames from base to next_seq-1.
            log(f"TIMEOUT for Frame {base}. Go-Back-N retransmission starts.", "bad")
            with state_lock: state["phase"] = f"Timeout → retransmitting {base}–{next_seq-1}"
            for seq in range(base, next_seq):
                if not wait_if_paused(): return
                transmit_frame(sock, seq, retransmission=True)
                time.sleep(0.85)
            continue

        with state_lock:
            state["last_ack"] = ack

        if ack >= base:
            old_base = base
            base = ack + 1
            log(f"Sender received cumulative ACK {ack}. Window slides {old_base} → {base}.", "ok")
            for i in range(old_base, base):
                set_frame("sender", i, "acked")

            # Fill the newly opened window. This is why after ACK 0 and ACK 1,
            # the sender can send 4 and 5 while 2 and 3 are still outstanding.
            while next_seq < min(base + window, total):
                if not wait_if_paused(): return
                transmit_frame(sock, next_seq)
                next_seq += 1
                with state_lock: state["next_seq"] = next_seq
                time.sleep(0.85)
        else:
            log(f"Duplicate ACK {ack} received; Frame {base} is still missing.", "warn")

    with state_lock:
        state["running"] = False
        state["complete"] = True
        state["phase"] = "Completed"
    log("All frames delivered successfully. Go-Back-N simulation complete.", "ok")
    try: sock.close()
    except: pass


def start_simulation():
    global sim_thread
    if sim_thread and sim_thread.is_alive():
        return
    stop_event.clear()
    pause_event.set()
    with state_lock:
        state["running"] = True
        state["paused"] = False
        state["complete"] = False
    sim_thread = threading.Thread(target=simulation, daemon=True)
    sim_thread.start()


class Handler(SimpleHTTPRequestHandler):
    def _send(self, code, data):
        raw = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/state":
            with state_lock: data = json.loads(json.dumps(state))
            self._send(200, data)
            return
        if path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def do_POST(self):
        path = urlparse(self.path).path
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b"{}"
        data = json.loads(body.decode() or "{}")
        if path == "/start":
            if not state["running"]:
                start_simulation()
            self._send(200, {"ok": True})
        elif path == "/pause":
            with state_lock:
                state["paused"] = True
                state["phase"] = "Paused"
            pause_event.clear()
            self._send(200, {"ok": True})
        elif path == "/resume":
            with state_lock:
                state["paused"] = False
            pause_event.set()
            self._send(200, {"ok": True})
        elif path == "/reset":
            stop_event.set(); pause_event.set()
            time.sleep(0.1)
            reset_state()
            self._send(200, {"ok": True})
        elif path == "/config":
            with state_lock:
                state["window"] = max(1, min(6, int(data.get("window", 4))))
                state["total"] = max(4, min(20, int(data.get("total", 10))))
                state["timeout"] = max(1000, min(5000, int(data.get("timeout", 2200))))
                state["loss_frames"] = generate_loss_frames(state["total"], state["window"])
                state["sender"] = [{"id": i, "status": "waiting"} for i in range(state["total"])]
                state["receiver"] = [{"id": i, "status": "waiting"} for i in range(state["total"])]
            self._send(200, {"ok": True})
        elif path == "/clear-log":
            with state_lock: state["logs"] = []
            self._send(200, {"ok": True})
        else:
            self._send(404, {"ok": False})

    def log_message(self, format, *args):
        pass


def main():
    threading.Thread(target=receiver_server, daemon=True).start()
    httpd = ThreadingHTTPServer((HOST, WEB_PORT), Handler)
    print(f"Go-Back-N web UI: http://{HOST}:{WEB_PORT}")
    print(f"Actual TCP Sender/Receiver socket: {HOST}:{TCP_PORT}")
    httpd.serve_forever()

if __name__ == "__main__":
    main()
