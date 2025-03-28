// queue.js - Task queue management using Bull
const Bull = require('bull');
const Redis = require('ioredis');
const { processScrapedData } = require('./process-data.js');

// Redis connection configuration
const redisConfig = {
  host: process.env.REDIS_HOST || 'redis', // Use 'redis' as default for Docker environment
  port: parseInt(process.env.REDIS_PORT || '6379'),
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
  retryStrategy: (times) => {
    const delay = Math.min(times * 50, 2000);
    console.log(`Redis connection retry attempt ${times} with delay ${delay}ms`);
    return delay;
  }
};

console.log(`Connecting to Redis at ${redisConfig.host}:${redisConfig.port}`);

// Create Redis clients
const redisClient = new Redis(redisConfig);
const redisSubscriber = new Redis(redisConfig);

// Increase max listeners to prevent warnings
require('events').EventEmitter.defaultMaxListeners = 50;

// Create Bull queues
const scrapeQueue = new Bull('scrape-tasks', {
  createClient: (type) => {
    switch (type) {
      case 'client':
        return redisClient;
      case 'subscriber':
        return redisSubscriber;
      default:
        return new Redis(redisConfig);
    }
  },
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 5000
    },
    removeOnComplete: 500,  // Keep last 500 completed jobs
    removeOnFail: 100,      // Keep last 100 failed jobs
    timeout: 30000          // 30 second timeout
  },
  settings: {
    stalledInterval: 15000,  // Check for stalled jobs every 15 seconds
    maxStalledCount: 2,      // Consider a job stalled after 2 checks
    lockDuration: 30000,     // Lock duration for jobs
    lockRenewTime: 15000     // Renew locks every 15 seconds
  }
});

// Create a results queue for storing scraping results
const resultsQueue = new Bull('scrape-results', {
  createClient: (type) => {
    switch (type) {
      case 'client':
        return redisClient;
      case 'subscriber':
        return redisSubscriber;
      default:
        return new Redis(redisConfig);
    }
  },
  defaultJobOptions: {
    removeOnComplete: 500,  // Keep last 500 completed jobs
    removeOnFail: 100       // Keep last 100 failed jobs
  }
});

// Set max listeners for the queues
scrapeQueue.setMaxListeners(50);
resultsQueue.setMaxListeners(50);

// Cache management
const memoryCache = {};

/**
 * Add a scraping task to the queue
 * @param {number} z - Zoom level
 * @param {number} x - X coordinate
 * @param {number} y - Y coordinate
 * @param {number} priority - Priority (lower is higher priority)
 * @param {string} jobId - Optional job ID for tracking
 * @returns {Promise<Bull.Job>} The created job
 */
async function addScrapeTask(z, x, y, priority = 10, jobId = null) {
  const cacheKey = `vessels-on-map:z=${z}:x=${x}:y=${y}`;

  // Check if we have cached data
  if (memoryCache[cacheKey] && memoryCache[cacheKey].expires > Date.now()) {
    console.log(`Memory cache hit for ${cacheKey}`);
    return { id: 'cached', data: memoryCache[cacheKey].data };
  }

  // Create a unique job ID if not provided
  const uniqueJobId = jobId || `scrape:z${z}:x${x}:y${y}:${Date.now()}`;

  // Add job to queue
  return scrapeQueue.add(
    { z, x, y, timestamp: Date.now() },
    {
      priority,
      jobId: uniqueJobId,
      timeout: 60000 // 60 second timeout
    }
  );
}

/**
 * Add multiple scraping tasks for a region
 * @param {number} z - Zoom level
 * @param {number} startX - Starting X coordinate
 * @param {number} startY - Starting Y coordinate
 * @param {number} width - Width of region in tiles
 * @param {number} height - Height of region in tiles
 * @returns {Promise<Array>} Array of created jobs
 */
async function addRegionTasks(z, startX, startY, width, height) {
  const jobs = [];
  const batchId = `batch:${Date.now()}`;

  for (let y = startY; y < startY + height; y++) {
    for (let x = startX; x < startX + width; x++) {
      const jobId = `${batchId}:z${z}:x${x}:y${y}`;
      const job = await addScrapeTask(z, x, y, 10, jobId);
      jobs.push(job);
    }
  }

  return jobs;
}

/**
 * Add tasks for all tiles at a specific zoom level
 * @param {number} z - Zoom level
 * @returns {Promise<Array>} Array of created jobs
 */
async function addAllTilesTasks(z) {
  const totalTiles = Math.pow(2, z);
  const batchId = `all:z${z}:${Date.now()}`;
  const jobs = [];

  for (let y = 0; y < totalTiles; y++) {
    for (let x = 0; x < totalTiles; x++) {
      const jobId = `${batchId}:z${z}:x${x}:y${y}`;
      const job = await addScrapeTask(z, x, y, 20, jobId);
      jobs.push(job);
    }
  }

  return jobs;
}

/**
 * Store results in cache
 * @param {number} z - Zoom level
 * @param {number} x - X coordinate
 * @param {number} y - Y coordinate
 * @param {Array} data - Vessel data
 * @param {number} ttl - Cache TTL in seconds
 */
function storeInCache(z, x, y, data, ttl = 300) {
  const cacheKey = `vessels-on-map:z=${z}:x=${x}:y=${y}`;
  memoryCache[cacheKey] = {
    data: data,
    expires: Date.now() + (ttl * 1000)
  };
  console.log(`Cached data for ${cacheKey} with TTL ${ttl}s (${data.length} vessels)`);
}

/**
 * Process scrape results from the results queue
 * @param {Object} job - Bull job containing scrape results
 */
async function processResults(job) {
  const { z, x, y, rawData } = job.data;

  try {
    // Process the raw data
    const vesselsData = processScrapedData(rawData, x, y);

    // Store in cache
    storeInCache(z, x, y, vesselsData);

    // Return the processed data
    return vesselsData;
  } catch (error) {
    console.error(`Error processing results for z=${z}, x=${x}, y=${y}:`, error);
    throw error;
  }
}

// Set up the results processor
resultsQueue.process(processResults);

// Export the queue functions
module.exports = {
  scrapeQueue,
  resultsQueue,
  addScrapeTask,
  addRegionTasks,
  addAllTilesTasks,
  storeInCache
};
