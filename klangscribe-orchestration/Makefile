.PHONY: up down build logs logs-storage clean restart status test

COMPOSE_FILE := docker/docker-compose.yml

# Start everythin
up:
	@echo "Starting Dagster..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ""
	@echo "Access Points:"
	@echo "  Dagster UI:    http://localhost:3000"
	@echo "  MinIO Console: https://localhost:9001"
	@echo "  PostgreSQL:    localhost:5432"

# Stop everything
down:
	docker compose -f $(COMPOSE_FILE) down

# Builder Dagster image
build:
	docker compose -f $(COMPOSE_FILE) build

# Rebuild and restart
rebuild:
	docker compose -f $(COMPOSE_FILE) down
	docker compose -f $(COMPOSE_FILE) build
	docker compose -f $(COMPOSE_FILE) up -d

# View Dagster logs
logs:
	docker compose -f $(COMPOSE_FILE) logs -f dagster-dev

# Check status
status:
	@echo "=== Dagster Service Status ==="
	@docker compose -f $(COMPOSE_FILE) ps -a
	@echo ""
	@echo "=== Port Usage ==="
	@echo "3000: Dagster UI"

# Restart Dagster (for code changes)
restart:
	docker compose -f $(COMPOSE_FILE) restart dagster-dev
	@echo "Dagster restarted"

# Clean everything including volumes
clean:
	docker compose -f $(COMPOSE_FILE) down -v
	docker system prune -f
	@echo "All services and volumes removed"

# Test connections
test:
	@echo "Testing Dagster connection..."
	@curl -s http://localhost:3000/server_info && echo "Dagster is healthy" || echo "Dagster is not responding"
