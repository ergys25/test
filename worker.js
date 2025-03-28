/**
 * Worker process for Marine Traffic scraper
 *
 * This file is the entry point for worker containers.
 * It connects to Redis, listens for scraping tasks,
 * executes them, and sends results back to the API.
 */

const Redis = require('ioredis');
const { scrapeMarineTraffic } = require('./scraper');

// Configuration from environment variables
const config = {
  // Redis Configuration
  REDIS_HOST: process.env.REDIS_HOST || 'localhost',
  REDIS_PORT: parseInt(process.env.REDIS_PORT || '6379'),

  // Worker Configuration
  WORKER_ID: process.env.WORKER_ID || '1',

  // Scraper Configuration
  SCRAPE_TIMEOUT: parseInt(process.env.SCRAPE_TIMEOUT || '30000') // 30 seconds (reduced from 60 seconds)
};

// Create Redis clients
const subscriber = new Redis({
  host: config.REDIS_HOST,
  port: config.REDIS_PORT
});

const publisher = new Redis({
  host: config.REDIS_HOST,
  port: config.REDIS_PORT
});

// Worker status
let isProcessing = false;
let currentTaskId = null;
let startTime = null;
let taskCount = 0;
let errorCount = 0;

// Handle graceful shutdown
process.on('SIGTERM', async () => {
  console.log(`Worker ${config.WORKER_ID} received SIGTERM, shutting down gracefully`);
  await cleanup();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log(`Worker ${config.WORKER_ID} received SIGINT, shutting down gracefully`);
  await cleanup();
  process.exit(0);
});

/**
 * Cleanup function for graceful shutdown
 */
async function cleanup() {
  // If currently processing a task, mark it as failed
  if (isProcessing && currentTaskId) {
    await publisher.publish('mt:results', JSON.stringify({
      taskId: currentTaskId,
      workerId: config.WORKER_ID,
      status: 'failed',
      error: 'Worker shutting down',
      timestamp: Date.now()
    }));
  }

  // Update worker status
  await publisher.publish('mt:worker:status', JSON.stringify({
    workerId: config.WORKER_ID,
    status: 'stopping',
    timestamp: Date.now()
  }));

  // Unsubscribe and close Redis connections
  await subscriber.unsubscribe('mt:tasks');
  await subscriber.quit();
  await publisher.quit();
}

/**
 * Process a scraping task
 * @param {Object} task - The task to process
 */
async function processTask(task) {
  if (isProcessing) {
    console.log(`Worker ${config.WORKER_ID} is already processing a task, ignoring new task ${task.taskId}`);
    return;
  }

  isProcessing = true;
  currentTaskId = task.taskId;
  startTime = Date.now();

  console.log(`Worker ${config.WORKER_ID} processing task ${task.taskId}: z=${task.z}, x=${task.x}, y=${task.y}`);

  // Update worker status
  await publisher.publish('mt:worker:status', JSON.stringify({
    workerId: config.WORKER_ID,
    status: 'processing',
    taskId: task.taskId,
    startTime,
    timestamp: Date.now()
  }));

  try {
    // Execute the scraping task with timeout
    const scrapePromise = scrapeMarineTraffic(task.z, task.x, task.y);
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new Error('Scraping timed out')), config.SCRAPE_TIMEOUT);
    });

    const result = await Promise.race([scrapePromise, timeoutPromise]);

    // Send successful result
    await publisher.publish('mt:results', JSON.stringify({
      taskId: task.taskId,
      workerId: config.WORKER_ID,
      status: 'completed',
      result,
      processingTime: Date.now() - startTime,
      timestamp: Date.now()
    }));

    console.log(`Worker ${config.WORKER_ID} completed task ${task.taskId} in ${Date.now() - startTime}ms`);
    taskCount++;
  } catch (error) {
    console.error(`Worker ${config.WORKER_ID} error processing task ${task.taskId}:`, error);

    // Send error result
    await publisher.publish('mt:results', JSON.stringify({
      taskId: task.taskId,
      workerId: config.WORKER_ID,
      status: 'failed',
      error: error.message,
      processingTime: Date.now() - startTime,
      timestamp: Date.now()
    }));

    errorCount++;
  } finally {
    // Reset worker state
    isProcessing = false;
    currentTaskId = null;

    // Update worker status
    await publisher.publish('mt:worker:status', JSON.stringify({
      workerId: config.WORKER_ID,
      status: 'idle',
      taskCount,
      errorCount,
      timestamp: Date.now()
    }));
  }
}

/**
 * Initialize the worker
 */
async function init() {
  console.log(`Worker ${config.WORKER_ID} starting...`);

  // Subscribe to the tasks channel
  await subscriber.subscribe('mt:tasks');

// Listen for tasks
subscriber.on('message', async (channel, message) => {
  if (channel === 'mt:tasks') {
    try {
      const task = JSON.parse(message);

      // Check if this task is for this worker
      if (task.workerId && task.workerId !== config.WORKER_ID) {
        return;
      }

      // If we're already processing a task, don't ignore it - queue it for later
      if (isProcessing) {
        console.log(`Worker ${config.WORKER_ID} is busy, queueing task ${task.taskId} for later processing`);

        // Let the API know we received the task but are busy
        await publisher.publish('mt:worker:status', JSON.stringify({
          workerId: config.WORKER_ID,
          status: 'busy',
          taskId: currentTaskId,
          queuedTaskId: task.taskId,
          timestamp: Date.now()
        }));

        // Process the task when we're done with the current one
        setTimeout(() => {
          processTask(task);
        }, 1000); // Check again in 1 second

        return;
      }

      await processTask(task);
    } catch (error) {
      console.error(`Worker ${config.WORKER_ID} error parsing task:`, error);
    }
  }
});

  // Announce worker is ready
  await publisher.publish('mt:worker:status', JSON.stringify({
    workerId: config.WORKER_ID,
    status: 'idle',
    taskCount: 0,
    errorCount: 0,
    timestamp: Date.now()
  }));

  console.log(`Worker ${config.WORKER_ID} ready and listening for tasks`);

  // Periodically send heartbeat (more frequently to detect issues faster)
  setInterval(async () => {
    await publisher.publish('mt:worker:heartbeat', JSON.stringify({
      workerId: config.WORKER_ID,
      status: isProcessing ? 'processing' : 'idle',
      taskId: currentTaskId,
      taskCount,
      errorCount,
      uptime: process.uptime(),
      timestamp: Date.now()
    }));
  }, 10000); // Every 10 seconds (reduced from 30 seconds)
}

// Start the worker
init().catch(error => {
  console.error(`Worker ${config.WORKER_ID} initialization error:`, error);
  process.exit(1);
});
