# Marine Traffic Distributed Scraping Service

A distributed system for scraping Marine Traffic vessel data using multiple concurrent browsers and services.

## Architecture

This service uses a distributed architecture with multiple components:

1. **Orchestrator**: Manages the API endpoints and distributes scraping tasks to workers
2. **Scrapers**: Multiple worker instances that perform the actual scraping
3. **Redis**: Message broker and cache for coordinating between services

```
┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │
│   Orchestrator  │◄────┤  API Requests   │
│                 │     │                 │
└────────┬────────┘     └─────────────────┘
         │
         │ (Redis Queue)
         ▼
┌─────────────────┐
│                 │
│  Redis Broker   │
│                 │
└─┬───────┬───────┘
  │       │
  │       │ (Task Distribution)
  │       │
  ▼       ▼
┌─────┐ ┌─────┐
│     │ │     │
│Scraper│ │Scraper│ ... (Multiple Scraper Instances)
│     │ │     │
└─────┘ └─────┘
```

## Features

- **Horizontal Scaling**: Add more scraper instances to increase throughput
- **Task Queuing**: Prioritized task queue with automatic retries
- **Distributed Caching**: Shared cache to avoid redundant scraping
- **Fault Tolerance**: Automatic recovery from failures
- **Browser Management**: Automatic browser health checks and restarts
- **Monitoring**: Status endpoint for monitoring queue health

## Configuration

The system can be configured through environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `SERVICE_ROLE` | Role of the service (`orchestrator` or `scraper`) | `orchestrator` |
| `HTTP_PORT` | Port for the HTTP server | `5090` |
| `MEMORY_CACHE_TTL` | Cache TTL in seconds | `300` |
| `MAX_CONCURRENT_JOBS` | Max concurrent jobs per scraper | `3` |
| `BROWSER_RESTART_INTERVAL` | Browser restart interval in ms | `3600000` |
| `JOB_TIMEOUT` | Job timeout in ms | `60000` |
| `JOB_RETRY_DELAY` | Delay between retries in ms | `5000` |
| `JOB_MAX_RETRIES` | Maximum number of retries | `3` |

## API Endpoints

### GET /vessels-on-map

Returns vessel data for a specific tile.

**Parameters:**
- `z` (optional): Zoom level (default: 7)
- `x` (optional): X coordinate (default: 73)
- `y` (optional): Y coordinate (default: 50)

### GET /traverse-tiles

Starts a background task to traverse multiple tiles.

**Parameters:**
- `z` (optional): Zoom level (default: 7)
- `startX` (optional): Starting X coordinate (default: 73)
- `startY` (optional): Starting Y coordinate (default: 50)
- `width` (optional): Width in tiles (default: 2)
- `height` (optional): Height in tiles (default: 2)
- `output` (optional): Output filename (default: vessel_data.json)

### GET /traverse-all

Starts a background task to traverse all tiles at the configured zoom level.

**Parameters:**
- `output` (optional): Output filename (default: vessel_data_z{zoom}_all.json)

### GET /status

Returns the status of the service, including queue statistics.

## Deployment

The service is deployed using Docker Compose:

```bash
# Build the Docker images
npm run docker:build

# Start the services
npm run docker:up

# Stop the services
npm run docker:down
```

## Scaling

To scale the number of scraper instances:

```bash
docker-compose up -d --scale scraper=5
```

This will run 5 scraper instances in parallel, increasing the throughput of the system.

## Performance Considerations

- Each scraper instance manages its own browser and can handle multiple concurrent pages
- The browser is automatically restarted periodically to prevent memory leaks
- Redis is used for both task queuing and result coordination
- Prioritized queuing ensures interactive requests are handled before background tasks
