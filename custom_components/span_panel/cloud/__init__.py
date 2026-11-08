"""SPAN cloud transport (unofficial): feeds this integration from SPAN's cloud.

SPAN's local eBus API only runs on the MAIN 32 for now. Until it reaches the
MAIN 40 and MAIN 16, the only way to read those panels is the cloud path the
SPAN Home app uses: Cognito login, gRPC to SPAN's mobile front end, and the
Ably push stream.

`span_client/` and `energy.py` are vendored from Michael Preissner's
ha-span-ebus (https://github.com/mpreissner/ha-span-ebus, MIT, see `LICENSE`),
as of vic-georgiev/ha-span-ebus@9b581f4: upstream's dev branch plus the breaker
details (spaces, rating, voltage) carried on each circuit's properties. The
throttle module from that branch is not included, and neither is `local/`.
One addition: the panel's feed-through lugs meter (PanelInstant #2), decoded in
`cloud_telemetry` and published as `panel/feedthrough_power` and `_current`.
Everything else in this package adapts that client to the contract the rest of
the integration already speaks (`span_panel_api`'s client protocols and
`SpanPanelSnapshot`), so the coordinator, the entities and the dashboard run
unmodified on top of it.
"""
