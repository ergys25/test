# Marine Traffic Vessel Data Service

A web service that scrapes Marine Traffic data for specific map tiles and returns it as JSON. The service includes in-memory caching to improve performance and reduce the number of requests to Marine Traffic.

## Installation

### Standard Installation

```bash
# Install dependencies
npm install

# Start the service
npm start
```

### Docker Installation

```bash
# Build the Docker image
npm run docker:build

# Start the service with Docker
npm run docker:up

# Stop the service
npm run docker:down
```

The service runs on port 3090 by default.

## API Endpoints

### 1. Get Vessels on a Specific Map Tile

```
GET /vessels-on-map?z=14&x=9242&y=6324
```

Parameters:
- `z`: Zoom level (default: 7)
- `x`: X coordinate (default: 73)
- `y`: Y coordinate (default: 50)

This endpoint returns vessel data for a specific map tile.

### 2. Traverse Multiple Tiles

```
GET /traverse-tiles?z=7&startX=73&startY=50&width=2&height=2&output=vessel_data.json
```

Parameters:
- `z`: Zoom level (default: 7)
- `startX`: Starting X coordinate (default: 73)
- `startY`: Starting Y coordinate (default: 50)
- `width`: Number of tiles to traverse horizontally (default: 2)
- `height`: Number of tiles to traverse vertically (default: 2)
- `output`: Output filename (default: 'vessel_data.json')

This endpoint traverses multiple tiles and saves the data to a file.

### 3. Traverse All Tiles at Zoom Level 3

```
GET /traverse-all
```

This endpoint traverses all tiles at zoom level 3 and returns the vessel data.

## Example Response

```json
[
  {
    "mmsi": 123456789,
    "timestamp": "1742815920426",
    "speed": 9.6,
    "cog": 65,
    "heading": 67,
    "lat": 37.97113,
    "lng": 23.96323,
    "a": 13,
    "b": 13,
    "c": 4,
    "d": 4,
    "reqts": "1742815921549",
    "shiptype": 70,
    "shipgroup": 70,
    "iso2": "gr",
    "country": null,
    "name": "VESSEL NAME",
    "destination": "PIRAEUS",
    "_tileX": "9242",
    "_tileY": "6324"
  },
  ...
]
```

## How It Works

The service uses puppeteer-stealth to navigate to specific tiles on Marine Traffic. It captures all XHR requests matching the pattern "station:0", extracts the vessel data from the responses, and returns the transformed vessel information as JSON.

## Response Data Fields

The vessel data includes the following fields:

- `mmsi`: Maritime Mobile Service Identity (unique vessel identifier)
- `timestamp`: Timestamp of the vessel's position
- `speed`: Speed in knots (divided by 10 from the original value)
- `cog`: Course over ground in degrees
- `heading`: Heading in degrees (may be null)
- `lat`: Latitude
- `lng`: Longitude
- `a`: Distance from bow to reference position in meters
- `b`: Distance from reference position to stern in meters
- `c`: Distance from port side to reference position in meters
- `d`: Distance from reference position to starboard side in meters
- `reqts`: Request timestamp
- `shiptype`: Type of vessel (numeric code)
- `shipgroup`: Ship group (same as shiptype)
- `iso2`: Country flag code (lowercase)
- `country`: Country name (may be null)
- `name`: Name of the vessel
- `destination`: Destination or "CLASS B" for AIS Class B vessels
- `_tileX`: X coordinate of the tile this vessel was found in
- `_tileY`: Y coordinate of the tile this vessel was found in

## Environment Variables

The service can be configured using the following environment variables:

- `HTTP_PORT`: Port to run the service on (default: 3090)
- `MEMORY_CACHE_TTL`: Time-to-live for in-memory cache in seconds (default: 300)

## Docker Deployment

The service includes Docker configuration files for easy deployment:

- `Dockerfile`: Defines the Docker image
- `docker-compose.yml`: Defines the Docker Compose configuration
- `.dockerignore`: Defines files to exclude from the Docker image

To deploy with Docker:

```bash
# Build and start the service
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the service
docker-compose down
```
