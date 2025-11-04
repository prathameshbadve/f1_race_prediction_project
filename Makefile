.PHONY: help build up down restart logs clean ps status minio-console mlflow dagster postgres-cli

# Default target
help:
	@echo "Available commands:"
	@echo "  make build-services                  - Build all Docker images"
	@echo "  make start-services                  - Start all services"
	@echo "  make stop-services                   - Stop all services"
	@echo "  make restart-services                - Restart all services"
	@echo "  make start-services-with-logs        - Start all services with logs"
	@echo "  make logs                            - View logs from all services"
	@echo "  make logs-<service>                  - View logs from specific service (e.g., make logs-mlflow)"
	@echo "  make ps                              - List running containers"
	@echo "  make status                          - Show status of all services"
	@echo "  make clean                           - Stop services and remove volumes (WARNING: deletes data)"
	@echo "  make clean-soft                      - Stop services but keep volumes"
	@echo "  make minio-console                   - Open MinIO console URL"
	@echo "  make mlflow                          - Open MLflow UI URL"
	@echo "  make dagster                         - Open Dagster UI URL"
	@echo "  make postgres-cli                    - Connect to PostgreSQL CLI"
	@echo "  make init                            - Initialize environment (first time setup)"

# Initialize environment (first time setup)
init:
	@echo "Initializing environment..."
	@cp .env.example .env 2>/dev/null || echo ".env file already exists"
	@mkdir -p dagster_home dagster_code
	@echo "Environment initialized! You can now run 'make build' and 'make up'"

# Build all images
build-services:
	@echo "Building Docker images..."
	docker-compose -f docker/docker-compose.yml build

# Start all services
start-services:
	@echo "Starting all services..."
	docker-compose -f docker/docker-compose.yml up -d
	@echo ""
	@echo "Services started! Access them at:"
	@echo "  MinIO Console:  http://localhost:9001 (user: minioadmin, pass: minioadmin)"
	@echo "  MLflow UI:      http://localhost:5000"
	@echo "  Dagster UI:     http://localhost:3000"
	@echo "  PostgreSQL:     localhost:5432 (user: postgres, pass: postgres)"

# Start services with logs
start-services-with-logs:
	@echo "Starting all services with logs..."
	docker-compose -f docker/docker-compose.yml up

# Stop all services
stop-services:
	@echo "Stopping all services..."
	docker-compose -f docker/docker-compose.yml down

# Restart all services
restart-services:
	@echo "Restarting all services..."
	docker-compose -f docker/docker-compose.yml restart

# View logs from all services
logs:
	docker-compose -f docker/docker-compose.yml logs -f

# View logs from specific service
logs-%:
	docker-compose -f docker/docker-compose.yml logs -f $*

# List running containers
ps:
	docker-compose -f docker/docker-compose.yml ps

# Show status of all services
status:
	@echo "Service Status:"
	@docker-compose ps

# Stop and remove everything including volumes (WARNING: deletes data)
clean:
	@echo "WARNING: This will delete all data in volumes!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		echo "Stopping services and removing volumes..."; \
		docker-compose -f docker/docker-compose.yml down -v; \
		echo "Cleanup complete!"; \
	else \
		echo "Cleanup cancelled."; \
	fi

# Stop services but keep volumes
clean-soft:
	@echo "Stopping services (keeping volumes)..."
	docker-compose -f docker/docker-compose.yml down
	@echo "Services stopped. Volumes preserved."

# Open MinIO console
minio-console:
	@echo "Opening MinIO Console at http://localhost:9001"
	@echo "Username: minioadmin"
	@echo "Password: minioadmin"

# Open MLflow UI
mlflow:
	@echo "Opening MLflow UI at http://localhost:5000"

# Open Dagster UI
dagster:
	@echo "Opening Dagster UI at http://localhost:3000"

# Connect to PostgreSQL CLI
postgres-cli:
	@echo "Connecting to PostgreSQL..."
	docker-compose -f docker/docker-compose.yml exec postgres psql -U postgres

# Check if services are healthy
health:
	@echo "Checking service health..."
	@docker-compose -f docker/docker-compose.yml ps | grep -E "(healthy|Up)"

# Rebuild specific service
rebuild-%:
	@echo "Rebuilding $*..."
	docker-compose -f docker/docker-compose.yml build $*
	docker-compose -f docker/docker-compose.yml up -d $*

# Restart specific service
restart-%:
	@echo "Restarting $*..."
	docker-compose -f docker/docker-compose.yml restart $*

# View resource usage
stats:
	@echo "Docker resource usage:"
	docker stats --no-stream

# Create MinIO buckets (if not already created)
create-buckets:
	@echo "Creating MinIO buckets..."
	docker-compose -f docker/docker-compose.yml run --rm minio-setup