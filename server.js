// server.js - API server and orchestrator
const express = require('express');
const fs = require('fs');
const path = require('path');
const {
  addScrapeTask,
  addRegionTasks,
  addAllTilesTasks
} = require('./queue.js');

// Determine if this instance is an orchestrator or a worker
const isOrchestrator = process.env.SERVICE_ROLE === 'orchestrator';
const isWorker = process.env.SERVICE_ROLE === 'scraper';

// If this is a worker, start the worker process
if (isWorker) {
  console.log('Starting in WORKER mode');
  require('./scraper-worker.js').startWorker()
    .catch(err => {
      console.error('Failed to start worker:', err);
      process.exit(1);
    });
  return; // Exit this file for worker instances
}

console.log('Starting in ORCHESTRATOR mode');

// --- Configuration ---
const config = {
  // Cache Configuration
  MEMORY_CACHE_TTL: parseInt(process.env.MEMORY_CACHE_TTL || '86400'), // 24 hours default TTL
  // Service Configuration
  HTTP_PORT: parseInt(process.env.HTTP_PORT || '5090'), // Default port
  // Scraping Configuration
  TRAVERSE_ALL_ZOOM: 3, // Fixed zoom level for /traverse-all
  TRAVERSE_ALL_TTL: 86400, // 24 hour cache TTL for zoom level 3 tiles
  // Job Configuration
  JOB_TIMEOUT: parseInt(process.env.JOB_TIMEOUT || '15000'), // 15 seconds default timeout per job
  JOB_RETRY_DELAY: parseInt(process.env.JOB_RETRY_DELAY || '2000'), // 2 seconds delay between retries
  JOB_MAX_RETRIES: parseInt(process.env.JOB_MAX_RETRIES || '2'), // Maximum number of retries per job
  // Queue Configuration
  MAX_CONCURRENT_REQUESTS: parseInt(process.env.MAX_CONCURRENT_REQUESTS || '50'), // Maximum concurrent requests
  QUEUE_STALE_CHECK_INTERVAL: parseInt(process.env.QUEUE_STALE_CHECK_INTERVAL || '30000') // Check for stale jobs every 30 seconds
};

// --- Global State ---
const app = express();
const PORT = process.env.PORT || config.HTTP_PORT;
let isShuttingDown = false;

// --- Express Middleware ---
app.use(express.json());

// CORS Middleware
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

// --- API Endpoints ---

/**
 * GET /vessels-on-map
 * Returns the vessel data for a specific tile, using distributed scraping.
 */
app.get('/vessels-on-map', async (req, res) => {
  try {
    const z = parseInt(req.query.z) || 7;
    const x = parseInt(req.query.x) || 73;
    const y = parseInt(req.query.y) || 50;

    console.log(`Received request for /vessels-on-map: z=${z}, x=${x}, y=${y}`);

    // Add task to queue and wait for result
    console.log(`Adding scrape task to queue: z=${z}, x=${x}, y=${y}`);
    const job = await addScrapeTask(z, x, y, 0); // Priority 0 (highest)
    console.log(`Task added to queue with job ID: ${job.id}`);

    // If we got a cached result, return it immediately
    if (job.id === 'cached') {
      console.log(`Returning cached data for z=${z}, x=${x}, y=${y}`);
      return res.json(job.data);
    }

    console.log(`Waiting for job ${job.id} to complete...`);

    // Wait for job to complete (with timeout)
    const result = await Promise.race([
      new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          console.error(`Job ${job.id} timed out after ${config.JOB_TIMEOUT}ms`);
          reject(new Error('Scraping operation timed out'));
        }, config.JOB_TIMEOUT);

        job.finished().then(jobResult => {
          clearTimeout(timeout);
          console.log(`Job ${job.id} completed successfully`);
          resolve(jobResult);
        }).catch(err => {
          clearTimeout(timeout);
          console.error(`Job ${job.id} failed:`, err);
          reject(err);
        });
      })
    ]);

    console.log(`Returning result for job ${job.id}, z=${z}, x=${x}, y=${y}`);
    return res.json(result);

  } catch (error) {
    console.error(`Error handling /vessels-on-map request (z=${req.query.z}, x=${req.query.x}, y=${req.query.y}):`, error);
    const statusCode = error.message.includes('timed out') ? 504 : 500;
    return res.status(statusCode).json({
      success: false,
      message: `Error retrieving tile data: ${error.message}`,
      error: error.toString()
    });
  }
});

/**
 * GET /traverse-tiles
 * Starts a distributed task to traverse multiple tiles.
 */
app.get('/traverse-tiles', async (req, res) => {
  try {
    const z = parseInt(req.query.z) || 7;
    const startX = parseInt(req.query.startX) || 73;
    const startY = parseInt(req.query.startY) || 50;
    const width = parseInt(req.query.width) || 2;
    const height = parseInt(req.query.height) || 2;
    const outputFile = req.query.output || 'vessel_data.json';

    console.log(`Starting /traverse-tiles request: z=${z}, startX=${startX}, startY=${startY}, width=${width}, height=${height}`);

    // Add region tasks to queue
    const jobs = await addRegionTasks(z, startX, startY, width, height);

    // Send immediate response
    res.json({
      success: true,
      message: 'Tile traversal started in the background.',
      params: { z, startX, startY, width, height, outputFile },
      status: `Processing ${jobs.length} tiles. Check server logs for progress and completion status.`,
      jobCount: jobs.length
    });

    // Start a background process to collect results and write to file
    collectAndSaveResults(jobs, outputFile)
      .catch(err => console.error("Error collecting results:", err));

  } catch (error) {
    console.error('Error starting /traverse-tiles:', error);
    return res.status(500).json({
      success: false,
      message: 'Error initiating tile traversal',
      error: error.message
    });
  }
});

/**
 * GET /traverse-all
 * Starts a distributed task to traverse all tiles at the configured zoom level.
 */
app.get('/traverse-all', async (req, res) => {
  try {
    const z = config.TRAVERSE_ALL_ZOOM;
    const outputFile = req.query.output || `vessel_data_z${z}_all.json`;

    console.log(`Starting /traverse-all request: z=${z}`);

    // Add all tiles tasks to queue
    const jobs = await addAllTilesTasks(z);

    // Send immediate response
    res.json({
      success: true,
      message: `Traversal of all tiles at zoom level ${z} started in the background.`,
      params: { z, outputFile },
      status: `Processing ${jobs.length} tiles. Check server logs for progress and completion status.`,
      jobCount: jobs.length
    });

    // Start a background process to collect results and write to file
    collectAndSaveResults(jobs, outputFile)
      .catch(err => console.error("Error collecting results:", err));

  } catch (error) {
    console.error('Error starting /traverse-all:', error);
    return res.status(500).json({
      success: false,
      message: 'Error initiating full tile traversal',
      error: error.message
    });
  }
});

/**
 * Collect results from jobs and save to file
 */
async function collectAndSaveResults(jobs, outputFile) {
  try {
    console.log(`Starting to collect results for ${jobs.length} jobs`);
    const allVesselsData = [];
    const uniqueVesselIds = new Set();

    // Wait for all jobs to complete
    for (const job of jobs) {
      try {
        // Skip already processed cached results
        if (job.id === 'cached') {
          const vesselsData = job.data;

          // Add unique vessels
          if (vesselsData && vesselsData.length > 0) {
            vesselsData.forEach(vessel => {
              if (vessel.mmsi && !uniqueVesselIds.has(vessel.mmsi)) {
                uniqueVesselIds.add(vessel.mmsi);
                allVesselsData.push(vessel);
              }
            });
          }

          continue;
        }

        // Wait for job to complete
        const result = await job.finished();

        // Add unique vessels
        if (result && result.length > 0) {
          result.forEach(vessel => {
            if (vessel.mmsi && !uniqueVesselIds.has(vessel.mmsi)) {
              uniqueVesselIds.add(vessel.mmsi);
              allVesselsData.push(vessel);
            }
          });
        }
      } catch (error) {
        console.error(`Error processing job result:`, error);
        // Continue with next job
      }
    }

    // Write the data to a file
    const outputPath = path.join(__dirname, outputFile);
    fs.writeFileSync(outputPath, JSON.stringify(allVesselsData, null, 2));
    console.log(`Collection complete. Found ${allVesselsData.length} unique vessels.`);
    console.log(`Data saved to ${outputPath}`);

  } catch (error) {
    console.error('Error collecting and saving results:', error);
  }
}

/**
 * GET /status
 * Returns the status of the service.
 */
app.get('/status', async (req, res) => {
  try {
    // Get queue statistics
    const scrapeQueueStats = await require('./queue.js').scrapeQueue.getJobCounts();
    const resultsQueueStats = await require('./queue.js').resultsQueue.getJobCounts();

    res.json({
      success: true,
      status: 'running',
      scrapeQueue: scrapeQueueStats,
      resultsQueue: resultsQueueStats,
      config: {
        cacheTTL: config.MEMORY_CACHE_TTL,
        jobTimeout: config.JOB_TIMEOUT,
        traverseAllZoom: config.TRAVERSE_ALL_ZOOM
      }
    });
  } catch (error) {
    console.error('Error getting status:', error);
    res.status(500).json({
      success: false,
      message: 'Error getting service status',
      error: error.message
    });
  }
});

/**
 * GET /health
 * Simple health check endpoint that doesn't require Redis
 */
app.get('/health', (req, res) => {
  res.json({
    success: true,
    status: 'healthy',
    role: isOrchestrator ? 'orchestrator' : 'worker',
    timestamp: new Date().toISOString(),
    uptime: process.uptime()
  });
});

/**
 * GET /redis-test
 * Test Redis connection
 */
app.get('/redis-test', async (req, res) => {
  try {
    // Create a test Redis client
    const Redis = require('ioredis');
    const redisConfig = {
      host: process.env.REDIS_HOST || 'redis',
      port: parseInt(process.env.REDIS_PORT || '6379')
    };

    console.log(`Testing Redis connection to ${redisConfig.host}:${redisConfig.port}`);
    const testClient = new Redis(redisConfig);

    // Set a test value
    const testKey = `test:${Date.now()}`;
    const testValue = `test-value-${Date.now()}`;
    await testClient.set(testKey, testValue, 'EX', 60); // Expire in 60 seconds

    // Get the test value back
    const retrievedValue = await testClient.get(testKey);

    // Close the test client
    await testClient.quit();

    res.json({
      success: true,
      message: 'Redis connection test successful',
      testKey,
      testValue,
      retrievedValue,
      match: testValue === retrievedValue
    });
  } catch (error) {
    console.error('Redis connection test failed:', error);
    res.status(500).json({
      success: false,
      message: 'Redis connection test failed',
      error: error.message
    });
  }
});

// --- Server Startup and Shutdown ---
async function startServer() {
  const server = app.listen(PORT, () => {
    console.log(`Marine Traffic Orchestrator Service running on port ${PORT}`);
    console.log(`Using config: Cache TTL=${config.MEMORY_CACHE_TTL}s, Job Timeout=${config.JOB_TIMEOUT / 1000}s`);
  });

  // Graceful shutdown
  const shutdown = async (signal) => {
    if (isShuttingDown) return;
    isShuttingDown = true;
    console.log(`\nReceived ${signal}. Shutting down gracefully...`);

    // Stop accepting new connections
    server.close(async () => {
      console.log('HTTP server closed.');
      process.exit(0); // Exit process
    });

    // Force close server after a timeout if connections linger
    setTimeout(() => {
      console.error('Could not close connections in time, forcefully shutting down');
      process.exit(1);
    }, 10000); // 10 seconds timeout
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

startServer().catch(err => {
  console.error("Failed to start server:", err);
  process.exit(1);
});
