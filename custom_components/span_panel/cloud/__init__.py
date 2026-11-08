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
Additions: the panel's feed-through lugs meter (PanelInstant #2), decoded in
`cloud_telemetry` and published as `panel/feedthrough_power` and `_current`;
and from the trait snapshot, each circuit's backup priority and PowerUp rank
(trait 1/31) and the panel's main breaker (1/21) and PowerUp limits (1/32),
decoded in `cloud_traits` and published as readings on every re-read.

What each trait field means, and the evidence for it, is written down in
docs/CLOUD-TRAITS.md of vic-georgiev/ha-span-ebus (branch vic/trait-inventory):
https://github.com/vic-georgiev/ha-span-ebus/blob/vic/trait-inventory/docs/CLOUD-TRAITS.md
Everything else in this package adapts that client to the contract the rest of
the integration already speaks (`span_panel_api`'s client protocols and
`SpanPanelSnapshot`), so the coordinator, the entities and the dashboard run
unmodified on top of it.
"""
