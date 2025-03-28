/**
 * Scraper Worker for Marine Traffic Service
 *
 * This module implements a worker that processes scraping requests from the queue.
 * Each worker runs independently and can handle one request at a time.
 */

const { scrapeMarineTraffic } = require('./scraper');
const queue = require('./queue');
const EventEmitter = require('events');

class ScraperWorker extends EventEmitter {
  constructor(workerId = null) {
    super();
    this.workerId = workerId || queue.registerWorker();
    this.currentRequestId = null;
    this.isRunning = false;
    this.isStopping = false;
    this.lastActivity = Date.now();
    this.stats = {
      requestsProcessed: 0,
      requestsFailed: 0,
      totalProcessingTime: 0
    };

    console.log(`Scraper worker ${this.workerId} initialized`);
  }

  /**
   * Start the worker
   */
  start() {
    if (this.isRunning) {
      console.log(`Worker ${this.workerId} is already running`);
      return;
    }

    this.isRunning = true;
    this.isStopping = false;
    console.log(`Worker ${this.workerId} started`);

    // Start processing requests
    this.processNextRequest();
  }

  /**
   * Stop the worker
   * @param {boolean} immediate - Whether to stop immediately or after current request
   */
  stop(immediate = false) {
    if (!this.isRunning) {
      console.log(`Worker ${this.workerId} is not running`);
      return;
    }

    this.isStopping = true;
    console.log(`Worker ${this.workerId} stopping (immediate: ${immediate})`);

    if (immediate && this.currentRequestId) {
      // Fail the current request
      queue.failRequest(
        this.currentRequestId,
        this.workerId,
        new Error('Worker stopped'),
        true
      );
      this.currentRequestId = null;
    }

    if (!this.currentRequestId || immediate) {
      this.isRunning = false;
      this.isStopping = false;
      console.log(`Worker ${this.workerId} stopped`);
      this.emit('stopped');
    }
  }

  /**
   * Process the next request from the queue
   */
  processNextRequest() {
    if (!this.isRunning || this.isStopping) {
      return;
    }

    // Get the next request from the queue
    const nextRequest = queue.getNextRequest(this.workerId);

    if (!nextRequest) {
      // No requests available, wait and try again
      setTimeout(() => this.processNextRequest(), 1000);
      return;
    }

    this.currentRequestId = nextRequest.requestId;
    this.lastActivity = Date.now();

    console.log(`Worker ${this.workerId} processing request ${this.currentRequestId}`);

    // Process the request
    this.processRequest(nextRequest.request)
      .then(result => {
        // Complete the request
        queue.completeRequest(this.currentRequestId, this.workerId, result);

        // Update stats
        this.stats.requestsProcessed++;
        this.stats.totalProcessingTime += Date.now() - this.lastActivity;

        // Clear current request
        this.currentRequestId = null;

        // Check if we should stop
        if (this.isStopping) {
          this.isRunning = false;
          this.isStopping = false;
          console.log(`Worker ${this.workerId} stopped after completing request`);
          this.emit('stopped');
          return;
        }

        // Process next request
        this.processNextRequest();
      })
      .catch(error => {
        // Fail the request
        queue.failRequest(this.currentRequestId, this.workerId, error, true);

        // Update stats
        this.stats.requestsFailed++;

        // Clear current request
        this.currentRequestId = null;

        // Check if we should stop
        if (this.isStopping) {
          this.isRunning = false;
          this.isStopping = false;
          console.log(`Worker ${this.workerId} stopped after failing request`);
          this.emit('stopped');
          return;
        }

        // Process next request
        this.processNextRequest();
      });
  }

  /**
   * Process a scraping request
   * @param {Object} request - The request to process
   * @returns {Promise<Object>} The result of the request
   */
  async processRequest(request) {
    // Extract request parameters
    const { z, x, y } = request;

    console.log(`Worker ${this.workerId} scraping tile z=${z}, x=${x}, y=${y}`);

    try {
      // Call the scraper function
      const result = await scrapeMarineTraffic(z, x, y);

      // Check if the result is valid
      if (!result || result.requestCount === 0) {
        throw new Error('No data returned from scraper');
      }

      return result;
    } catch (error) {
      console.error(`Worker ${this.workerId} error scraping tile z=${z}, x=${x}, y=${y}:`, error);
      throw error;
    }
  }

  /**
   * Get the worker's current status
   * @returns {Object} The worker status
   */
  getStatus() {
    return {
      workerId: this.workerId,
      isRunning: this.isRunning,
      isStopping: this.isStopping,
      currentRequestId: this.currentRequestId,
      lastActivity: this.lastActivity,
      stats: this.stats
    };
  }
}

/**
 * Worker Pool for managing multiple scraper workers
 */
class WorkerPool extends EventEmitter {
  constructor(size = 12) {
    super();
    this.size = size;
    this.workers = new Map();
    this.isRunning = false;
  }

  /**
   * Start the worker pool
   */
  start() {
    if (this.isRunning) {
      console.log('Worker pool is already running');
      return;
    }

    console.log(`Starting worker pool with ${this.size} workers`);

    // Create and start workers
    for (let i = 1; i <= this.size; i++) {
      const worker = new ScraperWorker();
      this.workers.set(worker.workerId, worker);

      // Listen for worker events
      worker.on('stopped', () => {
        this.handleWorkerStopped(worker.workerId);
      });

      // Start the worker
      worker.start();
    }

    this.isRunning = true;
    console.log('Worker pool started');
    this.emit('started');
  }

  /**
   * Stop the worker pool
   * @param {boolean} immediate - Whether to stop immediately or after current requests
   */
  stop(immediate = false) {
    if (!this.isRunning) {
      console.log('Worker pool is not running');
      return;
    }

    console.log(`Stopping worker pool (immediate: ${immediate})`);

    // Stop all workers
    for (const worker of this.workers.values()) {
      worker.stop(immediate);
    }

    // If immediate, mark as stopped now
    if (immediate) {
      this.isRunning = false;
      this.workers.clear();
      console.log('Worker pool stopped immediately');
      this.emit('stopped');
    }
  }

  /**
   * Handle a worker stopping
   * @param {number} workerId - The worker ID
   */
  handleWorkerStopped(workerId) {
    // Remove the worker from the pool
    this.workers.delete(workerId);

    // Check if all workers have stopped
    if (this.workers.size === 0) {
      this.isRunning = false;
      console.log('All workers stopped, worker pool is now stopped');
      this.emit('stopped');
    }
  }

  /**
   * Get the worker pool status
   * @returns {Object} The worker pool status
   */
  getStatus() {
    return {
      size: this.size,
      activeWorkers: this.workers.size,
      isRunning: this.isRunning,
      workers: Array.from(this.workers.values()).map(worker => worker.getStatus())
    };
  }
}

// Create a singleton instance of the worker pool
const workerPool = new WorkerPool();

module.exports = {
  ScraperWorker,
  WorkerPool,
  workerPool
};
