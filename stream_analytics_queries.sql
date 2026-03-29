-- =============================================================================
-- CST8916 Week 10 Lab – Azure Stream Analytics Queries
-- =============================================================================
--
-- JOB INPUTS
-- ----------
--   Alias : clickstream-input
--   Type  : Event Hub
--   Hub   : clickstream   (the hub that Flask /track publishes to)
--   Format: JSON
--
-- JOB OUTPUTS
-- -----------
--   Alias : analytics-output
--   Type  : Event Hub
--   Hub   : analytics-output   (a second hub in the same namespace)
--   Format: JSON
--   Set the ANALYTICS_HUB_NAME App Service setting to "analytics-output"
--   so the Flask app consumes SA results and the dashboard shows live data.
--
-- DESIGN DECISIONS
-- ----------------
--   • Both queries share a single output alias (analytics-output).  A
--     query_type field is added to every output row so the Flask consumer
--     can route device-breakdown rows and spike-detection rows to the
--     correct dashboard panel without needing two separate outputs.
--
--   • TIMESTAMP BY [timestamp] is used in both queries.  The timestamp
--     field is added server-side (UTC ISO 8601) in Flask's /track handler,
--     making it a reliable event-time source that is immune to network
--     or client-clock skew.
--
--   • Query 1 uses a TumblingWindow(second, 30) – non-overlapping 30-second
--     slices.  Each window gives the marketing team a fresh snapshot of
--     desktop vs. mobile vs. tablet share.  Tumbling windows were chosen
--     because the business question asks for a "continuous breakdown",
--     not a smoothed average; every window is independent and complete.
--
--   • Query 2 uses a HoppingWindow(second, 60, 10) – a 60-second look-back
--     that slides every 10 seconds.  This produces frequent (every 10 s)
--     updates while still aggregating over a full minute, making short
--     bursts of activity clearly visible as spikes.  A pure tumbling 60-s
--     window would delay spike detection by up to a minute; the hop
--     reduces that lag to at most 10 seconds.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Query 1: Device-Type Breakdown
--
-- Business question: "Which device types are most active?"
--
-- Produces one output row per (deviceType, 30-second window).
-- The dashboard reads these rows, picks the latest completed window, and
-- renders a horizontal bar chart of desktop / mobile / tablet traffic share.
-- ---------------------------------------------------------------------------
SELECT
    deviceType                              AS deviceType,
    COUNT(*)                                AS event_count,
    'device_breakdown'                      AS query_type,
    System.Timestamp()                      AS window_end
INTO
    [analytics-output]
FROM
    [clickstream-input] TIMESTAMP BY [timestamp]
GROUP BY
    deviceType,
    TumblingWindow(second, 30)
;


-- ---------------------------------------------------------------------------
-- Query 2: Traffic Spike Detection
--
-- Business question: "Are there traffic spikes?"
--
-- Produces one output row per 10-second hop, counting all events in the
-- trailing 60-second window.  When event_count >= 15 the row is flagged
-- as a spike (is_spike = 1).  The threshold of 15 events / 60 s is a
-- reasonable starting point for a demo store; adjust to match observed
-- baseline traffic in production.
--
-- The dashboard shows the last 12 windows (~2 minutes) as a bar timeline
-- with spike windows highlighted in red.
-- ---------------------------------------------------------------------------
SELECT
    COUNT(*)                                AS event_count,
    'spike_detection'                       AS query_type,
    System.Timestamp()                      AS window_end,
    CASE
        WHEN COUNT(*) >= 15 THEN 1
        ELSE 0
    END                                     AS is_spike
INTO
    [analytics-output]
FROM
    [clickstream-input] TIMESTAMP BY [timestamp]
GROUP BY
    HoppingWindow(second, 60, 10)
;
