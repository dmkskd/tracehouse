# Build context: grafana-app-plugin/dist (produced by grafana-plugin-build).
FROM grafana/grafana:12.3.2
COPY --chown=472:0 . /var/lib/grafana/plugins/dmkskd-tracehouse-app/
