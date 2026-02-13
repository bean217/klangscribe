.PHONY: help \
	ray-start ray-stop ray-status

RAY_DASHBOARD_PORT := 8265
RAY_HEAD_PORT := 6379

# Default target
help:
	@echo "Ray Management Commands"
	@echo "  make ray-start		Start Ray head with dashboard-port=${RAY_DASHBOARD_PORT} and head-port=${RAY_HEAD_PORT}"
	@echo "	 make ray-stop		Terminate Ray head and dashboard"
	@echo "  make ray-status	View the status of the Ray cluster"

# ============================================================================
# Ray Services
# ============================================================================
ray-start:
	ray start --head \
		--dashboard-host=0.0.0.0 \
		--dashboard-port=$(RAY_DASHBOARD_PORT)
		--port=$(RAY_HEAD_PORT)

ray-stop:
	ray stop

ray-status:
	ray status
