# Go-Back-N ARQ Simulator — corrected version

This version follows the Go-Back-N behavior shown in the supplied Computer Networks PDF, pages 59–66.

## Run

1. Extract the ZIP.
2. Open CMD in the project folder.
3. Run:

```bash
python server.py
```

4. Open `http://127.0.0.1:8000`.

## Demonstration sequence

The default configuration intentionally loses Frames **2 and 3 on their first transmission** so the Go-Back-N behavior is easy to observe:

```text
Sender:   0 → 1 → 2 → 3
Receiver: 0 ✓   1 ✓   2 LOST   3 LOST
ACKs:     ACK 0, ACK 1

Sender window slides to 2–5:
Sender:   2 → 3 → 4 → 5
Receiver: 2/3/4/5 are not accepted until Frame 2 arrives.

Timeout for Frame 2:
Sender retransmits ALL outstanding frames:
2 → 3 → 4 → 5

Receiver accepts them in order:
2 ✓ → ACK 2
3 ✓ → ACK 3
4 ✓ → ACK 4
5 ✓ → ACK 5
```

This matches the core GBN rule in the supplied material: the receiver accepts frames only in order, later frames are discarded after a missing frame, and on timeout the sender retransmits all outstanding frames. The sender maintains the oldest unacknowledged frame and the next frame to send. See the supplied PDF pages 59–65.

## Files

- `server.py` — HTTP UI server + actual TCP sender/receiver socket implementation
- `index.html` — one-page Sender/Receiver UI
- `style.css` — UI styling
- `app.js` — browser UI and polling

## Socket programming

The Python backend creates a real TCP connection between the Sender and Receiver on port `9001`. The browser talks to the Python HTTP server only for controls/state display; the actual simulated data frames and ACKs travel through the Python TCP socket.

## Important GBN behavior

- Receiver window is effectively one frame.
- Receiver accepts only the expected sequence number.
- Out-of-order frames are discarded.
- ACKs are cumulative.
- The sender's window advances when ACKs arrive.
- A timeout for the oldest outstanding frame causes all outstanding frames to be retransmitted.
- The window is filled again as cumulative ACKs arrive.
