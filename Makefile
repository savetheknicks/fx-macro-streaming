.PHONY: up down run stop

# Start infra, then run all three streaming clients as separate processes.
run:
	@trap 'kill 0' EXIT INT TERM; \
	uv run python -m producers.fx_poller & \
	uv run python -m producers.fred_replay & \
	uv run python -m consumers.sink_consumer & \
	uv run python -m stream_processor.processor & \
	wait

up:
	docker compose up -d

down:
	docker compose down --volumes
