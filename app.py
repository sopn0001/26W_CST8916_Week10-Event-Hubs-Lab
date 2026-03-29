# CST8916 – Week 10 Lab: Clickstream Analytics with Azure Event Hubs
#
# This Flask app has two roles:
#   1. PRODUCER  – receives click events from the browser and sends them to Azure Event Hubs
#   2. CONSUMER  – reads the last N events from Event Hubs and serves a live dashboard
#
# Routes:
#   GET  /                  → serves the demo e-commerce store (client.html)
#   POST /track             → receives a click event and publishes it to Event Hubs
#   GET  /dashboard         → serves the live analytics dashboard (dashboard.html)
#   GET  /api/events        → returns recent raw events as JSON (polled by the dashboard)
#   GET  /api/analytics     → returns windowed analytics: device breakdown + spike detection

import os
import json
import threading
from datetime import datetime, timezone, timedelta

from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS

from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

# ---------------------------------------------------------------------------
# Configuration – read from environment variables so secrets never live in code
#
# Required on Azure App Service Application Settings:
#   EVENT_HUB_CONNECTION_STR  – primary connection string for the namespace
#   EVENT_HUB_NAME            – hub that receives raw clickstream events (default: clickstream)
#
# Optional (enables live Stream Analytics output on the dashboard):
#   ANALYTICS_HUB_NAME        – hub where Stream Analytics writes its output
#                               (e.g. "analytics-output"). When omitted the
#                               dashboard falls back to computing aggregates
#                               from the in-memory clickstream buffer.
# ---------------------------------------------------------------------------
CONNECTION_STR      = os.environ.get("EVENT_HUB_CONNECTION_STR", "")
EVENT_HUB_NAME      = os.environ.get("EVENT_HUB_NAME", "clickstream")
ANALYTICS_HUB_NAME  = os.environ.get("ANALYTICS_HUB_NAME", "")

# ---------------------------------------------------------------------------
# In-memory ring-buffers
# ---------------------------------------------------------------------------
_event_buffer = []          # Raw clickstream events (last 50)
_buffer_lock  = threading.Lock()
MAX_BUFFER    = 50

_analytics_buffer = []      # Stream Analytics output events (last 200)
_analytics_lock   = threading.Lock()
MAX_ANALYTICS     = 200


# ---------------------------------------------------------------------------
# Helper – send a single event dict to Azure Event Hubs
# ---------------------------------------------------------------------------
def send_to_event_hubs(event_dict: dict):
    """Serialize event_dict to JSON and publish it to Event Hubs."""
    if not CONNECTION_STR:
        app.logger.warning("EVENT_HUB_CONNECTION_STR is not set – skipping Event Hubs publish")
        return

    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME,
    )
    with producer:
        event_batch = producer.create_batch()
        event_batch.add(EventData(json.dumps(event_dict)))
        producer.send_batch(event_batch)


# ---------------------------------------------------------------------------
# Background consumer – clickstream events → _event_buffer
# ---------------------------------------------------------------------------
def _on_event(partition_context, event):
    body = event.body_as_str(encoding="UTF-8")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        data = {"raw": body}

    with _buffer_lock:
        _event_buffer.append(data)
        if len(_event_buffer) > MAX_BUFFER:
            _event_buffer.pop(0)

    partition_context.update_checkpoint(event)


def start_consumer():
    """Start the clickstream Event Hubs consumer in a background daemon thread."""
    if not CONNECTION_STR:
        app.logger.warning("EVENT_HUB_CONNECTION_STR is not set – consumer thread not started")
        return

    consumer = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=EVENT_HUB_NAME,
    )

    def run():
        with consumer:
            consumer.receive(on_event=_on_event, starting_position="-1")

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    app.logger.info("Clickstream Event Hubs consumer thread started")


# ---------------------------------------------------------------------------
# Background consumer – Stream Analytics output → _analytics_buffer
# ---------------------------------------------------------------------------
def _on_analytics_event(partition_context, event):
    """Callback for events emitted by the Stream Analytics job."""
    body = event.body_as_str(encoding="UTF-8")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        data = {"raw": body}

    with _analytics_lock:
        _analytics_buffer.append(data)
        if len(_analytics_buffer) > MAX_ANALYTICS:
            _analytics_buffer.pop(0)

    partition_context.update_checkpoint(event)


def start_analytics_consumer():
    """Start the analytics Event Hubs consumer (optional – only if ANALYTICS_HUB_NAME is set)."""
    if not CONNECTION_STR or not ANALYTICS_HUB_NAME:
        app.logger.info(
            "ANALYTICS_HUB_NAME not configured – "
            "dashboard will use local-buffer fallback for analytics"
        )
        return

    consumer = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=ANALYTICS_HUB_NAME,
    )

    def run():
        with consumer:
            consumer.receive(on_event=_on_analytics_event, starting_position="-1")

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    app.logger.info("Analytics Event Hubs consumer thread started (hub: %s)", ANALYTICS_HUB_NAME)


# ---------------------------------------------------------------------------
# Analytics helpers – used when SA output is not available (local fallback)
# ---------------------------------------------------------------------------
def _compute_device_breakdown(events: list) -> list:
    """Count events per device_type from the clickstream buffer."""
    counts: dict[str, int] = {}
    for e in events:
        dt = e.get("device_type") or e.get("deviceType") or "unknown"
        counts[dt] = counts.get(dt, 0) + 1
    return [
        {"deviceType": k, "event_count": v}
        for k, v in sorted(counts.items(), key=lambda x: -x[1])
    ]


def _compute_spike_windows(events: list, window_seconds: int = 10,
                            lookback_minutes: int = 2, spike_threshold: int = 3) -> list:
    """
    Bucket events into fixed-size time windows and flag windows that exceed
    spike_threshold as traffic spikes.  Mirrors the HoppingWindow logic in the
    Stream Analytics job (window_seconds == hop size).
    """
    now    = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=lookback_minutes)

    # Parse ISO timestamps from the buffer
    timestamps: list[datetime] = []
    for e in events:
        ts_str = e.get("timestamp")
        if not ts_str:
            continue
        try:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts >= cutoff:
                timestamps.append(ts)
        except ValueError:
            pass

    # Assign each timestamp to its window bucket
    bucket_counts: dict[int, int] = {}
    for ts in timestamps:
        bucket = int(ts.timestamp() // window_seconds) * window_seconds
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1

    # Build a contiguous list of windows across the lookback range
    min_bucket = int(cutoff.timestamp() // window_seconds) * window_seconds
    max_bucket = int(now.timestamp()    // window_seconds) * window_seconds

    windows = []
    for b in range(min_bucket, max_bucket + window_seconds, window_seconds):
        count = bucket_counts.get(b, 0)
        windows.append({
            "window_end":  datetime.fromtimestamp(
                               b + window_seconds, tz=timezone.utc
                           ).strftime("%H:%M:%S"),
            "event_count": count,
            "is_spike":    count >= spike_threshold,
        })

    return windows[-12:]   # keep the 12 most recent windows (~2 min at 10-s buckets)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return send_from_directory("templates", "client.html")


@app.route("/dashboard")
def dashboard():
    return send_from_directory("templates", "dashboard.html")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"}), 200


@app.route("/track", methods=["POST"])
def track():
    """
    Receive a click event from the browser and publish it to Event Hubs.

    Expected JSON body (all fields sent by client.html):
    {
        "event_type":  "page_view" | "product_click" | "add_to_cart" | "banner_click",
        "page":        "/products/shoes",
        "product_id":  "p_shoe_42",        (optional)
        "user_id":     "u_1234",
        "session_id":  "s_abc123",
        "device_type": "desktop" | "mobile" | "tablet",
        "browser":     "Chrome" | "Firefox" | "Safari" | "Edge" | "Opera",
        "os":          "Windows" | "macOS" | "Linux" | "Android" | "iOS"
    }
    """
    if not request.json:
        abort(400)

    body = request.json

    # Build the enriched event.  The three new fields (device_type, browser, os)
    # are now captured from the client and forwarded to Event Hubs so downstream
    # systems (Stream Analytics) can aggregate by device context.
    event = {
        "event_type":  body.get("event_type", "unknown"),
        "page":        body.get("page", "/"),
        "product_id":  body.get("product_id"),
        "user_id":     body.get("user_id", "anonymous"),
        "session_id":  body.get("session_id", ""),
        "device_type": body.get("device_type", "unknown"),
        "browser":     body.get("browser", "unknown"),
        "os":          body.get("os", "unknown"),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }

    send_to_event_hubs(event)

    with _buffer_lock:
        _event_buffer.append(event)
        if len(_event_buffer) > MAX_BUFFER:
            _event_buffer.pop(0)

    return jsonify({"status": "ok", "event": event}), 201


@app.route("/api/events", methods=["GET"])
def get_events():
    """
    Return recent raw clickstream events.
    The dashboard polls this endpoint every 2 seconds.

    Query param: ?limit=N  (default 20, max 50)
    """
    try:
        limit = min(int(request.args.get("limit", 20)), MAX_BUFFER)
    except ValueError:
        limit = 20

    with _buffer_lock:
        recent = list(_event_buffer[-limit:])

    summary: dict[str, int] = {}
    for e in recent:
        et = e.get("event_type", "unknown")
        summary[et] = summary.get(et, 0) + 1

    return jsonify({"events": recent, "summary": summary, "total": len(recent)}), 200


@app.route("/api/analytics", methods=["GET"])
def get_analytics():
    """
    Return windowed analytics for the two Stream Analytics insights:

      1. device_breakdown – event count per device type (desktop / mobile / tablet)
         Mirrors the output of the TumblingWindow(second, 30) SAQL query.

      2. spike_windows    – per-window event counts with spike flag
         Mirrors the output of the HoppingWindow(second, 60, 10) SAQL query.

    When ANALYTICS_HUB_NAME is configured and the Stream Analytics job is running,
    this endpoint returns the actual SA output received via the analytics consumer.
    Otherwise it falls back to computing equivalent aggregates from the local
    clickstream buffer so the dashboard always shows meaningful data.

    Response shape:
    {
        "device_breakdown": [{"deviceType": "desktop", "event_count": 42}, ...],
        "spike_windows":    [{"window_end": "12:00:10", "event_count": 8, "is_spike": false}, ...],
        "source":           "stream_analytics" | "local_buffer"
    }
    """
    with _analytics_lock:
        sa_events = list(_analytics_buffer)

    if sa_events:
        # --- Stream Analytics path -------------------------------------------
        # SA emits one JSON object per row per window.  Each row has a
        # "query_type" field added by the SAQL query so we can route it here.
        device_rows = [e for e in sa_events if e.get("query_type") == "device_breakdown"]
        spike_rows  = [e for e in sa_events if e.get("query_type") == "spike_detection"]

        # Device breakdown: use only the most-recent window (highest window_end)
        if device_rows:
            latest_window = max(r.get("window_end", "") for r in device_rows)
            device_breakdown = [
                {"deviceType": r.get("deviceType", "unknown"),
                 "event_count": r.get("event_count", 0)}
                for r in device_rows
                if r.get("window_end") == latest_window
            ]
        else:
            device_breakdown = []

        # Spike windows: chronological, last 12 entries
        if spike_rows:
            spike_windows = sorted(spike_rows, key=lambda r: r.get("window_end", ""))[-12:]
            spike_windows = [
                {"window_end":  r.get("window_end", "")[-8:],   # HH:MM:SS portion
                 "event_count": r.get("event_count", 0),
                 "is_spike":    bool(r.get("is_spike", 0))}
                for r in spike_windows
            ]
        else:
            spike_windows = []

        source = "stream_analytics"

    else:
        # --- Local-buffer fallback -------------------------------------------
        with _buffer_lock:
            events = list(_event_buffer)

        device_breakdown = _compute_device_breakdown(events)
        spike_windows    = _compute_spike_windows(events)
        source           = "local_buffer"

    return jsonify({
        "device_breakdown": device_breakdown,
        "spike_windows":    spike_windows,
        "source":           source,
    }), 200


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
# Start background threads when the module is imported so they also launch
# under gunicorn (which imports the module directly and never reaches
# the __main__ block below).
start_consumer()
start_analytics_consumer()

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8000)
