/**
 * Entry point for Marine Traffic Scraper Service
 *
 * This file determines whether to start the API server or a worker
 * based on the SERVICE_MODE environment variable.
 */

const { ImprovedWorker } = require('./improved-worker');
const { browserPool } = require('./scraper');
const { v4: uuidv4 } = require('uuid');

// Configuration from environment variables
const config = {
  // Redis Configuration
  REDIS_HOST: process.env.REDIS_HOST || 'localhost',
  REDIS_PORT: parseInt(process.env.REDIS_PORT || '6379'),

  // Service Configuration
  SERVICE_MODE: process.env.SERVICE_MODE || 'api',

  // Worker Configuration
  WORKER_ID: process.env.WORKER_ID || uuidv4()
};

console.log(`Starting Marine Traffic Service in ${config.SERVICE_MODE} mode`);

// Start the appropriate service based on the mode
if (config.SERVICE_MODE === 'worker') {
  console.log(`Starting worker with ID ${config.WORKER_ID}`);

  // Create and start a worker
  const worker = new ImprovedWorker(config);

  // Handle graceful shutdown
  const shutdownGracefully = async (signal) => {
    console.log(`Worker ${config.WORKER_ID} received ${signal}, shutting down gracefully`);

    // Set a timeout to force exit if graceful shutdown takes too long
    const forceExitTimeout = setTimeout(() => {
      console.error(`Graceful shutdown timed out after 25 seconds, forcing exit`);
      process.exit(1);
    }, 25000); // 25 seconds timeout (slightly less than the 30s stop_grace_period)

    try {
      // Stop the worker
      await worker.stop();

      // Close all browser instances in the pool
      console.log('Closing all browser instances in the pool');
      await browserPool.closeAll();

      // Clear the timeout and exit normally
      clearTimeout(forceExitTimeout);
      console.log(`Worker ${config.WORKER_ID} shutdown complete`);
      process.exit(0);
    } catch (error) {
      console.error(`Error during graceful shutdown:`, error);
      clearTimeout(forceExitTimeout);
      process.exit(1);
    }
  };

  process.on('SIGTERM', () => shutdownGracefully('SIGTERM'));
  process.on('SIGINT', () => shutdownGracefully('SIGINT'));

  // Start the worker
  worker.start().catch(error => {
    console.error(`Error starting worker:`, error);
    process.exit(1);
  });
} else {
  // Start the API server
  console.log('Starting API server');
  require('./server');
}
