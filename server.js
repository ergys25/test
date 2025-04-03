const express = require('express');
const fs = require('fs');
const path = require('path');
const Redis = require('ioredis');
const { v4: uuidv4 } = require('uuid');
const { Pool } = require('pg');
const ImprovedQueue = require('./improved-queue');
const { ImprovedWorkerPool } = require('./improved-worker');
const { browserPool } = require('./scraper'); // Import the browser pool

// Configuration from environment variables
const config = {
  // Cache Configuration
  MEMORY_CACHE_TTL: parseInt(process.env.MEMORY_CACHE_TTL || '1800'), // 30 minutes (increased from 5 minutes)
  MEMORY_CACHE_STALE_TTL: parseInt(process.env.MEMORY_CACHE_STALE_TTL || '86400'), // 24 hours for stale cache

  // Service Configuration
  HTTP_PORT: parseInt(process.env.HTTP_PORT || '5032'), // Default port changed to 5032

  // Redis Configuration
  REDIS_HOST: process.env.REDIS_HOST || 'localhost',
  REDIS_PORT: parseInt(process.env.REDIS_PORT || '6379'),

  // PostgreSQL Configuration
  DB_HOST: process.env.DB_HOST || 'localhost',
  DB_PORT: parseInt(process.env.DB_PORT || '5432'),
  DB_NAME: process.env.DB_NAME || 'marine_traffic',
  DB_USER: process.env.DB_USER || 'mt_user',
  DB_PASSWORD: process.env.DB_PASSWORD || 'mt_password',

  // Task Configuration
  TASK_TIMEOUT: parseInt(process.env.TASK_TIMEOUT || '120000'), // 2 minutes (increased from 30 seconds)
  TASK_PRIORITY_HIGH: 10,
  TASK_PRIORITY_NORMAL: 5,
  TASK_PRIORITY_LOW: 0,

  // Worker Configuration
  WORKER_COUNT: parseInt(process.env.WORKER_COUNT || '12'), // Number of worker containers

  // Service Mode
  SERVICE_MODE: process.env.SERVICE_MODE || 'api', // 'api' or 'worker'

  // Cache Warming Configuration
  CACHE_WARMING_ENABLED: process.env.CACHE_WARMING_ENABLED !== 'false', // Enable by default
  CACHE_WARMING_INTERVAL: parseInt(process.env.CACHE_WARMING_INTERVAL || '3600000'), // 1 hour
  POPULAR_TILES: process.env.POPULAR_TILES ? JSON.parse(process.env.POPULAR_TILES) : [


  ]
};

// Create Express app
const app = express();
const PORT = process.env.PORT || config.HTTP_PORT;

// Initialize PostgreSQL connection pool
const pgPool = new Pool({
  host: config.DB_HOST,
  port: config.DB_PORT,
  database: config.DB_NAME,
  user: config.DB_USER,
  password: config.DB_PASSWORD,
});

// Test database connection
pgPool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error('Error connecting to PostgreSQL database:', err);
  } else {
    console.log('Connected to PostgreSQL database');
  }
});

// Initialize in-memory cache
const memoryCache = {};

// Create improved queue
const queue = new ImprovedQueue(config);

// Create Redis clients for pub/sub
const publisher = new Redis({
  host: config.REDIS_HOST,
  port: config.REDIS_PORT
});

const subscriber = new Redis({
  host: config.REDIS_HOST,
  port: config.REDIS_PORT
});

// Map to track pending tasks
const pendingTasks = new Map();

// Worker status tracking
const workerStatus = new Map();

// Create worker pool if in API mode
let workerPool = null;
if (config.SERVICE_MODE === 'api' && process.env.LOCAL_WORKERS === 'true') {
  workerPool = new ImprovedWorkerPool(config, config.WORKER_COUNT);
}

// Handle graceful shutdown
const shutdownGracefully = async (signal) => {
  console.log(`${signal} received, shutting down gracefully`);

  // Set a timeout to force exit if graceful shutdown takes too long
  const forceExitTimeout = setTimeout(() => {
    console.error(`Graceful shutdown timed out after 25 seconds, forcing exit`);
    process.exit(1);
  }, 25000); // 25 seconds timeout (slightly less than the 30s stop_grace_period)

  try {
    await cleanup();

    // Clear the timeout and exit normally
    clearTimeout(forceExitTimeout);
    console.log('API server shutdown complete');
    process.exit(0);
  } catch (error) {
    console.error(`Error during graceful shutdown:`, error);
    clearTimeout(forceExitTimeout);
    process.exit(1);
  }
};

process.on('SIGTERM', () => shutdownGracefully('SIGTERM'));
process.on('SIGINT', () => shutdownGracefully('SIGINT'));

/**
 * Cleanup function for graceful shutdown
 */
async function cleanup() {
  console.log('Cleaning up before shutdown');

  // Stop worker pool if it exists
  if (workerPool && workerPool.isRunning) {
    console.log('Stopping worker pool');
    await workerPool.stop(true);
  }

  // Close all browser instances in the pool
  console.log('Closing all browser instances in the pool');
  await browserPool.closeAll();

  // Unsubscribe from Redis channels
  await subscriber.unsubscribe('mt:results', 'mt:worker:status', 'mt:worker:heartbeat');

  // Close Redis connections
  await subscriber.quit();
  await publisher.quit();
  await queue.close();

  // Close PostgreSQL connection pool
  console.log('Closing PostgreSQL connection pool');
  await pgPool.end();

  console.log('Cleanup complete');
}

// Initialize Redis subscribers
subscriber.subscribe('mt:results', 'mt:worker:status', 'mt:worker:heartbeat');

// Handle messages from Redis
subscriber.on('message', (channel, message) => {
  try {
    const data = JSON.parse(message);

    if (channel === 'mt:results') {
      handleTaskResult(data);
    } else if (channel === 'mt:worker:status') {
      updateWorkerStatus(data);
    } else if (channel === 'mt:worker:heartbeat') {
      updateWorkerHeartbeat(data);
    }
  } catch (error) {
    console.error(`Error handling Redis message on channel ${channel}:`, error);
  }
});

/**
 * Handle a task result from a worker
 * @param {Object} data - The result data
 */
function handleTaskResult(data) {
  const { taskId, status, result, error } = data;

  // Check if we have a pending task with this ID
  if (!pendingTasks.has(taskId)) {
    console.log(`Received result for unknown task ${taskId}`);
    return;
  }

  const task = pendingTasks.get(taskId);

  // Clear the timeout
  if (task.timeoutId) {
    clearTimeout(task.timeoutId);
  }

  // Handle the result
  if (status === 'completed') {
    console.log(`Task ${taskId} completed successfully by worker ${data.workerId}`);
    task.resolve(result);
  } else {
    console.error(`Task ${taskId} failed:`, error);
    task.reject(new Error(error || 'Unknown error'));
  }

  // Remove the task from pending
  pendingTasks.delete(taskId);
}

/**
 * Update worker status
 * @param {Object} data - The worker status data
 */
function updateWorkerStatus(data) {
  const { workerId, status } = data;

  // Update worker status
  workerStatus.set(workerId, {
    ...data,
    lastUpdate: Date.now()
  });

  console.log(`Worker ${workerId} status: ${status}`);
}

/**
 * Update worker heartbeat
 * @param {Object} data - The worker heartbeat data
 */
function updateWorkerHeartbeat(data) {
  const { workerId } = data;

  // Update worker status with heartbeat
  if (workerStatus.has(workerId)) {
    workerStatus.set(workerId, {
      ...workerStatus.get(workerId),
      ...data,
      lastHeartbeat: Date.now()
    });
  } else {
    workerStatus.set(workerId, {
      ...data,
      lastHeartbeat: Date.now(),
      lastUpdate: Date.now()
    });
  }
}

/**
 * Find an available worker
 * @returns {string|null} The ID of an available worker, or null if none available
 */
function findAvailableWorker() {
  // First try to find an idle worker
  const idleWorkers = [];
  for (const [workerId, status] of workerStatus.entries()) {
    if (status.status === 'idle') {
      idleWorkers.push(workerId);
    }
  }

  // If we have idle workers, return the one with the least tasks processed
  // This helps distribute load more evenly
  if (idleWorkers.length > 0) {
    idleWorkers.sort((a, b) => {
      const aStatus = workerStatus.get(a);
      const bStatus = workerStatus.get(b);
      return (aStatus.taskCount || 0) - (bStatus.taskCount || 0);
    });
    return idleWorkers[0];
  }

  // If no idle workers, return null
  return null;
}

// Track the last worker assigned to a task
let lastAssignedWorkerIndex = 0;

/**
 * Queue a task to be processed by a worker
 * @param {Object} task - The task to queue
 * @param {number} priority - The priority of the task (higher is more important)
 * @returns {Promise<Object>} - Promise that resolves with the task result
 */
function queueTask(task, priority = 0) {
  return new Promise((resolve, reject) => {
    // Generate a unique task ID
    const taskId = uuidv4();

    // Create the full task object
    const fullTask = {
      taskId,
      ...task,
      priority,
      timestamp: Date.now()
    };

    // Add task to pending tasks
    pendingTasks.set(taskId, {
      task: fullTask,
      priority,
      resolve,
      reject,
      queuedAt: Date.now(),
      timeoutId: setTimeout(() => {
        // Handle task timeout
        if (pendingTasks.has(taskId)) {
          console.error(`Task ${taskId} timed out`);
          reject(new Error('Task timed out'));
          pendingTasks.delete(taskId);
        }
      }, config.TASK_TIMEOUT)
    });

    // Add task to the improved queue
    queue.addTask(fullTask, priority)
      .then(() => {
        console.log(`Task ${taskId} added to queue with priority ${priority}`);
      })
      .catch(error => {
        console.error(`Error adding task ${taskId} to queue:`, error);

        // Remove from pending tasks
        if (pendingTasks.has(taskId)) {
          clearTimeout(pendingTasks.get(taskId).timeoutId);
          pendingTasks.delete(taskId);
          reject(error);
        }
      });
  });
}

// Helper function to compress data
function compressData(data, acceptEncoding) {
  // In a real implementation, this would compress the data based on accept-encoding
  // For now, we'll just return the data as is
  return { data, encoding: null };
}

// Helper function to set CORS headers
function setCorsHeaders(headers) {
  return {
    ...headers,
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };
}

/**
 * Set shipgroup to be the same as shiptype
 */
function determineShipGroup(mmsi, shipType) {
  // Convert shipType to number
  const shipTypeNum = parseInt(shipType);

  // If shipType is a valid number, return it as the shipgroup
  if (!isNaN(shipTypeNum)) {
    return shipTypeNum;
  }

  // If shipType is not a valid number, check MMSI
  if (mmsi && mmsi.toString().startsWith('99')) {
    return 1;
  }

  // Default
  return 0;
}

// Middleware to parse JSON
app.use(express.json());

// Start the worker pool if in API mode with local workers
if (workerPool) {
  workerPool.start()
    .then(() => {
      console.log(`Started local worker pool with ${config.WORKER_COUNT} workers`);
    })
    .catch(error => {
      console.error('Error starting worker pool:', error);
    });
}

// Track tasks that are currently being processed to prevent duplicates
const tasksInProgress = new Map();

/**
 * Queue a scraping request and return a promise that resolves when the request is complete
 * @param {number} z - Zoom level
 * @param {number} x - X coordinate
 * @param {number} y - Y coordinate
 * @param {number} priority - Priority of the request (higher is more important)
 * @returns {Promise<Object>} - Promise that resolves with the scraping result
 */
const queueScrapeRequest = (z, x, y, priority = 0) => {
  // Create a unique key for this task
  const taskKey = `scrape:${z}:${x}:${y}`;

  // Check if this task is already in progress
  if (tasksInProgress.has(taskKey)) {
    console.log(`Task ${taskKey} is already in progress, reusing existing promise`);
    return tasksInProgress.get(taskKey);
  }

  // Create the task object
  const task = {
    type: 'scrape',
    z,
    x,
    y
  };

  // Create a promise for this task
  const taskPromise = queueTask(task, priority);

  // Store the promise in the tasksInProgress map
  tasksInProgress.set(taskKey, taskPromise);

  // Remove the task from the map when it completes or fails
  taskPromise
    .then(result => {
      tasksInProgress.delete(taskKey);
      return result;
    })
    .catch(error => {
      tasksInProgress.delete(taskKey);
      throw error;
    });

  // Return the promise
  return taskPromise;
};

/**
 * Extract vessel data from raw response data
 */
function extractVesselData(data, tileX, tileY, z) {
  const vesselsMap = new Map();

  // Check if data is available
  if (!data) {
    console.log('No data to extract vessels from');
    return [];
  }

  // Ensure data is an array
  if (!Array.isArray(data)) {
    console.log('Data is not an array, cannot extract vessels');
    return [];
  }

  // Process each item in the data array
  data.forEach(item => {
    if (item.response && item.response.data) {
      const responseData = item.response.data;

      if (responseData.type === 1 && responseData.data && responseData.data.rows) {
        responseData.data.rows.forEach(vessel => {
          const currentTime = Date.now();
          const elapsedMs = parseInt(vessel.ELAPSED || '0') * 1000;
          const timestamp = (currentTime - elapsedMs).toString();

          // Log if this is a station 0 request
          if (item.request && item.request.url && item.request.url.includes('station=0')) {
            console.log(`Found station 0 request: ${item.request.url}`);
          }

          const mt_id = vessel.SHIP_ID ? parseInt(vessel.SHIP_ID) : null;
          const transformedVessel = {
            mt_id: mt_id,
            timestamp: timestamp,
            speed: vessel.SPEED ? parseFloat(vessel.SPEED) / 10 : 0, // Convert speed to knots (divide by 10)
            cog: vessel.COURSE ? parseFloat(vessel.COURSE) : null,
            heading: vessel.HEADING ? parseFloat(vessel.HEADING) : null,
            lat: vessel.LAT ? parseFloat(vessel.LAT) : null,
            lng: vessel.LON ? parseFloat(vessel.LON) : null,
            a: vessel.L_FORE ? parseInt(vessel.L_FORE) : null,
            b: vessel.LENGTH && vessel.L_FORE ? parseInt(vessel.LENGTH) - parseInt(vessel.L_FORE) : null,
            c: vessel.W_LEFT ? parseInt(vessel.W_LEFT) : null,
            d: vessel.WIDTH && vessel.W_LEFT ? parseInt(vessel.WIDTH) - parseInt(vessel.W_LEFT) : null,
            reqts: currentTime.toString(),
            shiptype: vessel.SHIPTYPE ? parseInt(vessel.SHIPTYPE) : null,
            shipgroup: determineShipGroup(vessel.SHIP_ID, vessel.SHIPTYPE),
            iso2: vessel.FLAG ? vessel.FLAG.toLowerCase() : null,
            country: null,
            name: vessel.SHIPNAME || null,
            destination: vessel.DESTINATION || null,
            _tileX: (item.request && item.request.url && item.request.url.match(/X:(\d+)/)?.[1]) || tileX.toString(),
            _tileY: (item.request && item.request.url && item.request.url.match(/Y:(\d+)/)?.[1]) || tileY.toString(),
            _zoom: z.toString(),
            zoom: z // Add zoom to main response object
          };

          // Only add vessel if it's not already in the map or if this is a newer position
          if (!vesselsMap.has(mt_id) ||
              vesselsMap.get(mt_id).timestamp < transformedVessel.timestamp) {
            vesselsMap.set(mt_id, transformedVessel);
          }
        });
      }
    }
  });

  // Convert Map values to array before returning
  return Array.from(vesselsMap.values());
}

/**
 * Enrich vessel data with IMO and MMSI from the database
 * @param {Array} vessels - Array of vessel objects
 * @returns {Promise<Array>} - Promise that resolves with enriched vessel array
 */
async function enrichVesselsWithDbData(vessels) {
  if (!vessels || vessels.length === 0) {
    return [];
  }

  try {
    // Extract all ship_ids from the vessels
    const shipIds = vessels.map(vessel => vessel.mt_id).filter(id => id !== null);

    if (shipIds.length === 0) {
      console.log('No valid ship IDs to query in the database');
      return vessels;
    }

    // Query the database for vessels with matching ship_ids
    const query = {
      text: 'SELECT ship_id, imo, mmsi FROM vessels WHERE ship_id = ANY($1)',
      values: [shipIds]
    };

    const result = await pgPool.query(query);
    console.log(`Found ${result.rows.length} matching vessels in the database`);

    // Create a map of ship_id to database vessel data for quick lookup
    const dbVesselsMap = new Map();
    result.rows.forEach(row => {
      dbVesselsMap.set(row.ship_id, {
        imo: row.imo,
        mmsi: row.mmsi
      });
    });

    // Enrich vessels with database data and filter out non-matching vessels
    const enrichedVessels = vessels.filter(vessel => {
      if (vessel.mt_id && dbVesselsMap.has(vessel.mt_id)) {
        const dbData = dbVesselsMap.get(vessel.mt_id);
        vessel.imo = dbData.imo;
        vessel.mmsi = dbData.mmsi;
        return true;
      }
      return false;
    });

    console.log(`Filtered vessels from ${vessels.length} to ${enrichedVessels.length} after database matching`);
    return enrichedVessels;
  } catch (error) {
    console.error('Error enriching vessels with database data:', error);
    return vessels; // Return original vessels in case of error
  }
}

/**
 * Deduplicate vessels by MMSI
 * @param {Array} vessels - Array of vessel objects
 * @returns {Array} - Deduplicated array of vessel objects
 */
function deduplicateVessels(vessels) {
  // Use a Map to deduplicate vessels by MMSI
  const vesselsMap = new Map();

  vessels.forEach(vessel => {
    const mmsi = vessel.mmsi || vessel.mt_id; // Use MMSI or mt_id as the key

    // Only add vessel if it's not already in the map or if this is a newer position
    if (!vesselsMap.has(mmsi) ||
        vesselsMap.get(mmsi).timestamp < vessel.timestamp) {
      vesselsMap.set(mmsi, vessel);
    }
  });

  // Convert Map values to array before returning
  return Array.from(vesselsMap.values());
}

/**
 * GET /vessels-on-map
 * Returns the vessel data for a specific tile
 * Query parameters:
 * - z: Zoom level (default: 7)
 * - x: X coordinate (default: 73)
 * - y: Y coordinate (default: 50)
 * - refresh: Force refresh the cache (default: false)
 * - priority: Priority of the request (default: 0)
 */
app.get('/vessels-on-map', async (req, res) => {
  try {
    const startTime = Date.now();

    // Get tile coordinates from query parameters
    const z = parseInt(req.query.z) || 7;
    const x = parseInt(req.query.x) || 73;
    const y = parseInt(req.query.y) || 50;
    const forceRefresh = req.query.refresh === 'true';
    const priority = parseInt(req.query.priority) || config.TASK_PRIORITY_NORMAL;

    console.log(`Received request for tile z=${z}, x=${x}, y=${y} with priority ${priority}, forceRefresh=${forceRefresh}`);

    const cacheKey = `vessels-on-map:z=${z}:x=${x}:y=${y}`;

    // Initialize cache entry if it doesn't exist
    if (!memoryCache[cacheKey]) {
      memoryCache[cacheKey] = { data: [], expires: 0, refreshing: false };
    }

    const cacheEntry = memoryCache[cacheKey];
    const now = Date.now();
    const isFresh = cacheEntry.expires > now;
    const isStale = !isFresh && cacheEntry.data && cacheEntry.data.length > 0;

    // If we have fresh data and not forcing refresh, return it immediately
    if (isFresh && !forceRefresh) {
      const responseTime = Date.now() - startTime;
      console.log(`Memory cache hit for ${cacheKey}, returned in ${responseTime}ms`);

      // Deduplicate vessels before returning
      const dedupedData = deduplicateVessels(cacheEntry.data);

      // Enrich with database data and filter out non-matching vessels
      const enrichedData = await enrichVesselsWithDbData(dedupedData);

      return res.json(enrichedData);
    }

    // If we have stale data, return it immediately and refresh in background
    if (isStale && !cacheEntry.refreshing) {
      // Mark as refreshing to prevent multiple refresh requests
      cacheEntry.refreshing = true;

      // Return stale data immediately
      const responseTime = Date.now() - startTime;
      console.log(`Returning stale data for ${cacheKey} while refreshing in background, returned in ${responseTime}ms`);

      // Refresh in background
      refreshCacheInBackground(z, x, y, priority);

      // Deduplicate vessels before returning
      const dedupedData = deduplicateVessels(cacheEntry.data);

      // Enrich with database data and filter out non-matching vessels
      const enrichedData = await enrichVesselsWithDbData(dedupedData);

      return res.json({
        data: enrichedData,
        stale: true
      });
    }

    // If we don't have any data or forcing refresh, fetch new data
    console.log(`No cache available for ${cacheKey} or force refresh requested, queuing scrape request`);

    try {
      // Queue the scraping request
      const data = await queueScrapeRequest(z, x, y, priority);

      // Extract vessel data
      const vesselsData = extractVesselData(data, x, y, z);

      // Deduplicate vessels before storing in cache
      const dedupedVessels = deduplicateVessels(vesselsData);

      // Store in memory cache with TTL from config
      memoryCache[cacheKey] = {
        data: dedupedVessels,
        expires: now + (config.MEMORY_CACHE_TTL * 1000), // Convert seconds to milliseconds
        refreshing: false
      };

      const responseTime = Date.now() - startTime;
      console.log(`Cached data for ${cacheKey} with TTL ${config.MEMORY_CACHE_TTL}s (${vesselsData.length} vessels), returned in ${responseTime}ms`);

      // Deduplicate vessels before returning
      const dedupedData = deduplicateVessels(vesselsData);

      // Enrich with database data and filter out non-matching vessels
      const enrichedData = await enrichVesselsWithDbData(dedupedData);

      return res.json(enrichedData);
    } catch (error) {
      console.error(`Error scraping data for tile z=${z}, x=${x}, y=${y}:`, error);

      // Reset refreshing flag
      if (cacheEntry) {
        cacheEntry.refreshing = false;
      }

      // Check if we have stale cache data we can return
      if (cacheEntry && cacheEntry.data && cacheEntry.data.length > 0) {
        console.log(`Returning stale cache data for ${cacheKey} after error`);

        // Deduplicate vessels before returning
        const dedupedData = deduplicateVessels(cacheEntry.data);

        // Enrich with database data and filter out non-matching vessels
        const enrichedData = await enrichVesselsWithDbData(dedupedData);

        return res.json({
          data: enrichedData,
          stale: true,
          error: error.message
        });
      }

      throw error; // Re-throw to be caught by the outer catch block
    }
  } catch (error) {
    console.error('Error handling tile data request:', error);
    return res.status(500).json({
      success: false,
      message: 'Error retrieving tile data',
      error: error.message
    });
  }
});

/**
 * Refresh cache in background without blocking the response
 */
async function refreshCacheInBackground(z, x, y, priority) {
  const cacheKey = `vessels-on-map:z=${z}:x=${x}:y=${y}`;

  try {
    console.log(`Background refresh started for ${cacheKey}`);

    // Queue the scraping request with lower priority
    const data = await queueScrapeRequest(z, x, y, Math.max(0, priority - 2));

    // Extract vessel data
    const vesselsData = extractVesselData(data, x, y, z);

    // Deduplicate vessels before storing in cache
    const dedupedVessels = deduplicateVessels(vesselsData);

    // Enrich with database data and filter out non-matching vessels
    const enrichedVessels = await enrichVesselsWithDbData(dedupedVessels);

    // Store in memory cache with TTL from config
    memoryCache[cacheKey] = {
      data: enrichedVessels, // Store the enriched data in cache
      expires: Date.now() + (config.MEMORY_CACHE_TTL * 1000), // Convert seconds to milliseconds
      refreshing: false
    };

    console.log(`Background refresh completed for ${cacheKey} (${vesselsData.length} vessels)`);
  } catch (error) {
    console.error(`Error in background refresh for ${cacheKey}:`, error);

    // Reset refreshing flag
    if (memoryCache[cacheKey]) {
      memoryCache[cacheKey].refreshing = false;
    }
  }
}

// CORS middleware
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

/**
 * GET /traverse-tiles
 * Traverses multiple tiles and collects vessel data
 * Query parameters:
 * - z: Zoom level (default: 7)
 * - startX: Starting X coordinate (default: 73)
 * - startY: Starting Y coordinate (default: 50)
 * - width: Number of tiles to traverse horizontally (default: 2)
 * - height: Number of tiles to traverse vertically (default: 2)
 * - output: Output filename (default: 'vessel_data.json')
 */
app.get('/traverse-tiles', async (req, res) => {
  try {
    // Get parameters from query
    const z = parseInt(req.query.z) || 7;
    const startX = parseInt(req.query.startX) || 73;
    const startY = parseInt(req.query.startY) || 50;
    const width = parseInt(req.query.width) || 2;
    const height = parseInt(req.query.height) || 2;
    const outputFile = req.query.output || 'vessel_data.json';
    const priority = parseInt(req.query.priority) || 0;

    console.log(`Starting tile traversal: z=${z}, startX=${startX}, startY=${startY}, width=${width}, height=${height}`);

    // Create a response to send immediately
    res.json({
      success: true,
      message: 'Tile traversal started',
      params: { z, startX, startY, width, height, outputFile, priority },
      status: 'Processing tiles in the background. Check the server logs for progress.'
    });

    // Process tiles in the background
    processTilesInBackground(z, startX, startY, width, height, outputFile, priority);

  } catch (error) {
    console.error('Error starting tile traversal:', error);
    return res.status(500).json({
      success: false,
      message: 'Error starting tile traversal',
      error: error.message
    });
  }
});

/**
 * Process multiple tiles in the background and save the data to a file
 */
async function processTilesInBackground(z, startX, startY, width, height, outputFile, priority = 0) {
  try {
    console.log(`Processing ${width}x${height} tiles starting at (${startX},${startY}) with zoom ${z}`);

    // Array to store all vessel data
    const allVesselsData = [];

    // Set to track unique vessel IDs to avoid duplicates
    const uniqueVesselIds = new Set();

    // Create an array of all tile coordinates to process
    const tilesToProcess = [];
    for (let y = startY; y < startY + height; y++) {
      for (let x = startX; x < startX + width; x++) {
        tilesToProcess.push({ z, x, y });
      }
    }

    // Process tiles in batches to avoid overwhelming the queue
    const BATCH_SIZE = Math.min(config.WORKER_COUNT, tilesToProcess.length);
    console.log(`Processing tiles in batches of ${BATCH_SIZE}`);

    for (let i = 0; i < tilesToProcess.length; i += BATCH_SIZE) {
      const batch = tilesToProcess.slice(i, i + BATCH_SIZE);
      console.log(`Processing batch ${Math.floor(i / BATCH_SIZE) + 1} of ${Math.ceil(tilesToProcess.length / BATCH_SIZE)}`);

      // Process batch in parallel
      const batchPromises = batch.map(async (tile) => {
        try {
          const { z, x, y } = tile;
          console.log(`Queuing tile z=${z}, x=${x}, y=${y}`);

          // Check memory cache first
          const cacheKey = `vessels-on-map:z=${z}:x=${x}:y=${y}`;
          let vesselsData = null;

          // Check if we have a valid in-memory cache entry
          if (memoryCache[cacheKey] && memoryCache[cacheKey].expires > Date.now()) {
            console.log(`Memory cache hit for ${cacheKey}`);
            vesselsData = memoryCache[cacheKey].data;
            return vesselsData;
          }

          // If no cached data, queue a scrape request
          console.log(`No cache available for ${cacheKey}, queuing scrape request`);

          // Queue the scraping request (timeout is handled in queueTask)
          const data = await queueScrapeRequest(z, x, y, priority);

          // Extract vessel data
          vesselsData = extractVesselData(data, x, y, z);

          // Deduplicate vessels before storing in cache
          const dedupedVessels = deduplicateVessels(vesselsData);

          // Store in memory cache
          memoryCache[cacheKey] = {
            data: dedupedVessels,
            expires: Date.now() + (config.MEMORY_CACHE_TTL * 1000) // Convert seconds to milliseconds
          };

          console.log(`Cached data for ${cacheKey} with TTL ${config.MEMORY_CACHE_TTL}s (${vesselsData.length} vessels)`);

          return vesselsData;
        } catch (tileError) {
          console.error(`Error processing tile z=${tile.z}, x=${tile.x}, y=${tile.y}:`, tileError);
          return []; // Return empty array for failed tiles
        }
      });

      // Wait for all batch promises to resolve
      const batchResults = await Promise.all(batchPromises);

      // Collect all vessels from batch results
      let allBatchVessels = [];
      batchResults.forEach(vesselsData => {
        if (vesselsData && vesselsData.length > 0) {
          allBatchVessels = allBatchVessels.concat(vesselsData);
        }
      });

      // Deduplicate vessels using our deduplication function
      const dedupedBatchVessels = deduplicateVessels(allBatchVessels);

      // Add to the overall results
      allVesselsData = allVesselsData.concat(dedupedBatchVessels);

      console.log(`Processed batch ${Math.floor(i / BATCH_SIZE) + 1}, found ${allVesselsData.length} unique vessels so far`);
    }

    // Write the data to a file
    const outputPath = path.join(__dirname, outputFile);
    fs.writeFileSync(outputPath, JSON.stringify(allVesselsData, null, 2));

    console.log(`Tile traversal complete. Found ${allVesselsData.length} unique vessels.`);
    console.log(`Data saved to ${outputPath}`);
  } catch (error) {
    console.error('Error in background tile processing:', error);
  }
}

// API endpoints for worker management
/**
 * GET /workers/status
 * Returns the current status of all workers
 */
app.get('/workers/status', (req, res) => {
  try {
    const status = Array.from(workerStatus.entries()).map(([workerId, status]) => ({
      workerId,
      ...status
    }));

    return res.json({
      workers: status,
      pendingTasks: pendingTasks.size
    });
  } catch (error) {
    console.error('Error getting worker status:', error);
    return res.status(500).json({
      success: false,
      message: 'Error getting worker status',
      error: error.message
    });
  }
});

/**
 * GET /tasks/pending
 * Returns the current pending tasks
 */
app.get('/tasks/pending', (req, res) => {
  try {
    const tasks = Array.from(pendingTasks.entries()).map(([taskId, task]) => ({
      taskId,
      queuedAt: task.queuedAt,
      priority: task.priority,
      task: task.task
    }));

    return res.json({
      count: tasks.length,
      tasks
    });
  } catch (error) {
    console.error('Error getting pending tasks:', error);
    return res.status(500).json({
      success: false,
      message: 'Error getting pending tasks',
      error: error.message
    });
  }
});

/**
 * GET /traverse-all
 * Traverses all tiles at zoom level 3 and returns the vessel data
 * No parameters required
 */
app.get('/traverse-all', async (req, res) => {
  try {
    console.log('Starting traversal of all tiles at zoom level 3');

    // Call the function that collects all vessel data
    const allVesselsData = await traverseAllTiles();

    // Return the collected data
    return res.json({
      success: true,
      message: 'Full tile traversal completed',
      count: allVesselsData.length,
      vessels: allVesselsData
    });

  } catch (error) {
    console.error('Error in full tile traversal:', error);
    return res.status(500).json({
      success: false,
      message: 'Error in full tile traversal',
      error: error.message
    });
  }
});

/**
 * Traverse all tiles at zoom level 3 and return all vessel data
 * @returns {Promise<Array>} Array of vessel data
 */
async function traverseAllTiles() {
  try {
    const z = 3; // Fixed zoom level

    // At zoom level 3, there are 2^3 = 8 tiles in each direction
    const totalTiles = Math.pow(2, z);

    console.log(`Starting traversal of all ${totalTiles}x${totalTiles} tiles at zoom level ${z}`);

    // Array to store all vessel data
    const allVesselsData = [];

    // Set to track unique vessel IDs to avoid duplicates
    const uniqueVesselIds = new Set();

    // Collect all vessels from all tiles
    let allTilesVessels = [];

    // Process each tile in the grid
    for (let y = 0; y < totalTiles; y++) {
      for (let x = 0; x < totalTiles; x++) {
        console.log(`Processing tile z=${z}, x=${x}, y=${y} (${(y * totalTiles + x + 1)} of ${totalTiles * totalTiles})`);

        try {
          // Check memory cache first
          const cacheKey = `vessels-on-map:z=${z}:x=${x}:y=${y}`;
          let vesselsData = null;

          // Check if we have a valid in-memory cache entry
          if (memoryCache[cacheKey] && memoryCache[cacheKey].expires > Date.now()) {
            console.log(`Memory cache hit for ${cacheKey}`);
            vesselsData = memoryCache[cacheKey].data;
          }

          // If no cached data, queue a scrape request
          if (!vesselsData) {
            console.log(`No cache available for ${cacheKey}, queuing scrape request`);

            // Queue the scraping request (timeout is handled in queueTask)
            const data = await queueScrapeRequest(z, x, y, 10); // Higher priority for traverse-all

            // Extract vessel data
            vesselsData = extractVesselData(data, x, y, z);

            // Deduplicate vessels before storing in cache
            const dedupedVessels = deduplicateVessels(vesselsData);

            // Store in memory cache with a longer TTL for zoom level 3
            const longerTTL = 3600; // 1 hour
            memoryCache[cacheKey] = {
              data: dedupedVessels,
              expires: Date.now() + (longerTTL * 1000) // Convert seconds to milliseconds
            };

            console.log(`Cached data for ${cacheKey} with TTL ${longerTTL}s (${vesselsData.length} vessels)`);
          }

          // Add vessels to the collection
          if (vesselsData && vesselsData.length > 0) {
            allTilesVessels = allTilesVessels.concat(vesselsData);
          }

          console.log(`Processed tile z=${z}, x=${x}, y=${y}, found ${vesselsData ? vesselsData.length : 0} vessels`);

          // Wait a bit between tiles to avoid rate limiting
          await new Promise(resolve => setTimeout(resolve, 2000));

        } catch (tileError) {
          console.error(`Error processing tile z=${z}, x=${x}, y=${y}:`, tileError);
          // Continue with next tile
        }
      }
    }

    // Deduplicate all vessels using our deduplication function
    const dedupedVessels = deduplicateVessels(allTilesVessels);

    console.log(`Full tile traversal complete. Found ${dedupedVessels.length} unique vessels across all tiles.`);

    // Return the deduplicated vessel data
    return dedupedVessels;

  } catch (error) {
    console.error('Error in full tile traversal:', error);
    throw error; // Re-throw the error to be caught by the endpoint handler
  }
}

// Track tiles that are currently being warmed to prevent duplicate requests
const tilesBeingWarmed = new Set();

/**
 * Cache warming function to proactively fetch and cache popular tiles
 */
async function warmCache() {
  if (!config.CACHE_WARMING_ENABLED) {
    return;
  }

  console.log('Starting cache warming for popular tiles...');

  // Process each popular tile
  for (const tile of config.POPULAR_TILES) {
    const { z, x, y } = tile;
    const cacheKey = `vessels-on-map:z=${z}:x=${x}:y=${y}`;
    const tileId = `${z}-${x}-${y}`;

    // Skip if this tile is already being warmed
    if (tilesBeingWarmed.has(tileId)) {
      console.log(`Tile z=${z}, x=${x}, y=${y} is already being warmed, skipping`);
      continue;
    }

    // Check if we need to refresh this tile
    const needsRefresh = !memoryCache[cacheKey] ||
                         !memoryCache[cacheKey].data ||
                         memoryCache[cacheKey].expires < (Date.now() + config.CACHE_WARMING_INTERVAL / 2);

    if (needsRefresh) {
      console.log(`Warming cache for tile z=${z}, x=${x}, y=${y}`);

      // Mark this tile as being warmed
      tilesBeingWarmed.add(tileId);

      try {
        // Queue with low priority to avoid blocking user requests
        const data = await queueScrapeRequest(z, x, y, config.TASK_PRIORITY_LOW);

        // Extract vessel data
        const vesselsData = extractVesselData(data, x, y, z);

        // Deduplicate vessels before storing in cache
        const dedupedVessels = deduplicateVessels(vesselsData);

        // Enrich with database data and filter out non-matching vessels
        const enrichedVessels = await enrichVesselsWithDbData(dedupedVessels);

        // Store in memory cache with TTL from config
        memoryCache[cacheKey] = {
          data: enrichedVessels, // Store the enriched data in cache
          expires: Date.now() + (config.MEMORY_CACHE_TTL * 1000),
          refreshing: false
        };

        console.log(`Warmed cache for ${cacheKey} with ${vesselsData.length} vessels`);
      } catch (error) {
        console.error(`Error warming cache for tile z=${z}, x=${x}, y=${y}:`, error);
      } finally {
        // Remove the tile from the being warmed set
        tilesBeingWarmed.delete(tileId);
      }

      // Wait a bit between tiles to avoid overwhelming the system
      await new Promise(resolve => setTimeout(resolve, 5000));
    } else {
      console.log(`Cache for tile z=${z}, x=${x}, y=${y} is still fresh, skipping`);
    }
  }

  console.log('Cache warming complete');
}

// Start the server
app.listen(PORT, () => {
  console.log(`Marine Traffic Tile Data Service running on port ${PORT}`);

  // Start cache warming if enabled
  if (config.CACHE_WARMING_ENABLED) {
    console.log(`Cache warming enabled, will run every ${config.CACHE_WARMING_INTERVAL / 60000} minutes`);

    // Initial cache warming after a short delay
    setTimeout(() => warmCache(), 10000);

    // Set up interval for regular cache warming
    setInterval(() => warmCache(), config.CACHE_WARMING_INTERVAL);
  }
});
