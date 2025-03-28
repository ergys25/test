/**
 * Improved Worker for Marine Traffic Scraper
 *
 * This module implements a worker that pulls tasks from the queue when available.
 * Each worker runs independently and can handle one request at a time.
 */

const Redis = require('ioredis');
const { v4: uuidv4 } = require('uuid');
const { scrapeMarineTraffic, browserPool } = require('./scraper');
const ImprovedQueue = require('./improved-queue');

class ImprovedWorker {
  constructor(config) {
    this.config = config;
    this.workerId = config.WORKER_ID || uuidv4();
    this.queue = new ImprovedQueue(config);

    // Create Redis client for publishing status updates
    this.publisher = new Redis({
      host: config.REDIS_HOST,
      port: config.REDIS_PORT
    });

    // Worker state
    this.isRunning = false;
    this.isStopping = false;
    this.currentTaskId = null;
    this.stats = {
      tasksProcessed: 0,
      tasksFailed: 0,
      totalProcessingTime: 0,
      startTime: Date.now()
    };

    // Polling interval (ms)
    this.pollingInterval = 100; // Poll frequently for tasks
    this.pollingIntervalId = null;

    // Heartbeat interval (ms)
    this.heartbeatInterval = 10000; // Every 10 seconds
    this.heartbeatIntervalId = null;

    console.log(`Worker ${this.workerId} initialized`);
  }

  /**
   * Start the worker
   */
  async start() {
    if (this.isRunning) {
      console.log(`Worker ${this.workerId} is already running`);
      return;
    }

    this.isRunning = true;
    this.isStopping = false;
    this.stats.startTime = Date.now();

    // Announce worker is ready
    await this.updateStatus('idle');

    // Start polling for tasks
    this.pollingIntervalId = setInterval(() => this.pollForTasks(), this.pollingInterval);

    // Start heartbeat
    this.heartbeatIntervalId = setInterval(() => this.sendHeartbeat(), this.heartbeatInterval);

    console.log(`Worker ${this.workerId} started and polling for tasks`);
  }

  /**
   * Stop the worker
   * @param {boolean} immediate - Whether to stop immediately or after current task
   */
  async stop(immediate = false) {
    if (!this.isRunning) {
      console.log(`Worker ${this.workerId} is not running`);
      return;
    }

    this.isStopping = true;
    console.log(`Worker ${this.workerId} stopping (immediate: ${immediate})`);

    // Stop polling for new tasks
    if (this.pollingIntervalId) {
      clearInterval(this.pollingIntervalId);
      this.pollingIntervalId = null;
    }

    // Stop heartbeat
    if (this.heartbeatIntervalId) {
      clearInterval(this.heartbeatIntervalId);
      this.heartbeatIntervalId = null;
    }

    // If we have a current task and immediate stop is requested
    if (immediate && this.currentTaskId) {
      await this.queue.failTask(
        this.currentTaskId,
        new Error('Worker stopping'),
        this.workerId,
        true
      );
      this.currentTaskId = null;
    }

    // If no current task or immediate stop
    if (!this.currentTaskId || immediate) {
      await this.updateStatus('stopping');
      this.isRunning = false;
      this.isStopping = false;

      // Close connections
      await this.queue.close();
      await this.publisher.quit();

      console.log(`Worker ${this.workerId} stopped`);
    }
  }

  /**
   * Poll for available tasks
   */
  async pollForTasks() {
    if (!this.isRunning || this.isStopping || this.currentTaskId) {
      return;
    }

    try {
      // Try to get a task from the queue
      const task = await this.queue.getNextTask();

      if (!task) {
        // No tasks available
        return;
      }

      // We got a task, process it
      this.currentTaskId = task.taskId;

      // Update status
      await this.updateStatus('processing', {
        taskId: task.taskId,
        taskType: task.type,
        startTime: Date.now()
      });

      console.log(`Worker ${this.workerId} processing task ${task.taskId}`);

      // Process the task
      this.processTask(task).catch(error => {
        console.error(`Worker ${this.workerId} error processing task ${task.taskId}:`, error);
      });
    } catch (error) {
      console.error(`Worker ${this.workerId} error polling for tasks:`, error);
    }
  }

  /**
   * Process a task
   * @param {Object} task - The task to process
   */
  async processTask(task) {
    const startTime = Date.now();

    try {
      let result;

      // Process based on task type
      if (task.type === 'scrape') {
        // Scrape Marine Traffic
        result = await scrapeMarineTraffic(task.z, task.x, task.y);
      } else {
        throw new Error(`Unknown task type: ${task.type}`);
      }

      // Calculate processing time
      const processingTime = Date.now() - startTime;

      // Update stats
      this.stats.tasksProcessed++;
      this.stats.totalProcessingTime += processingTime;

      // Complete the task
      await this.queue.completeTask(task.taskId, result, this.workerId);

      console.log(`Worker ${this.workerId} completed task ${task.taskId} in ${processingTime}ms`);
    } catch (error) {
      // Update stats
      this.stats.tasksFailed++;

      // Fail the task
      await this.queue.failTask(task.taskId, error, this.workerId, true);

      console.error(`Worker ${this.workerId} failed task ${task.taskId}:`, error);
    } finally {
      // Clear current task
      this.currentTaskId = null;

      // Update status
      await this.updateStatus('idle');

      // If stopping, complete the stop
      if (this.isStopping) {
        await this.stop(true);
      }
    }
  }

  /**
   * Update worker status
   * @param {string} status - The new status
   * @param {Object} additionalInfo - Additional info to include
   */
  async updateStatus(status, additionalInfo = {}) {
    const statusUpdate = {
      workerId: this.workerId,
      status,
      timestamp: Date.now(),
      ...additionalInfo,
      stats: this.stats
    };

    await this.publisher.publish('mt:worker:status', JSON.stringify(statusUpdate));
  }

  /**
   * Send heartbeat
   */
  async sendHeartbeat() {
    const heartbeat = {
      workerId: this.workerId,
      status: this.currentTaskId ? 'processing' : 'idle',
      timestamp: Date.now(),
      uptime: Math.floor((Date.now() - this.stats.startTime) / 1000),
      stats: this.stats,
      currentTaskId: this.currentTaskId
    };

    await this.publisher.publish('mt:worker:heartbeat', JSON.stringify(heartbeat));
  }

  /**
   * Get worker status
   * @returns {Object} The worker status
   */
  getStatus() {
    return {
      workerId: this.workerId,
      isRunning: this.isRunning,
      isStopping: this.isStopping,
      currentTaskId: this.currentTaskId,
      stats: this.stats
    };
  }
}

/**
 * Worker Pool for managing multiple workers
 */
class ImprovedWorkerPool {
  constructor(config, size = 12) {
    this.config = config;
    this.size = size;
    this.workers = new Map();
    this.isRunning = false;
  }

  /**
   * Start the worker pool
   */
  async start() {
    if (this.isRunning) {
      console.log('Worker pool is already running');
      return;
    }

    console.log(`Starting worker pool with ${this.size} workers`);

    // Create and start workers
    for (let i = 1; i <= this.size; i++) {
      const workerConfig = {
        ...this.config,
        WORKER_ID: `worker-${i}`
      };

      const worker = new ImprovedWorker(workerConfig);
      this.workers.set(worker.workerId, worker);

      // Start the worker
      await worker.start();
    }

    this.isRunning = true;
    console.log('Worker pool started');
  }

  /**
   * Stop the worker pool
   * @param {boolean} immediate - Whether to stop immediately or after current tasks
   */
  async stop(immediate = false) {
    if (!this.isRunning) {
      console.log('Worker pool is not running');
      return;
    }

    console.log(`Stopping worker pool (immediate: ${immediate})`);

    // Stop all workers
    const stopPromises = [];
    for (const worker of this.workers.values()) {
      stopPromises.push(worker.stop(immediate));
    }

    // Wait for all workers to stop
    await Promise.all(stopPromises);

    this.isRunning = false;
    this.workers.clear();
    console.log('Worker pool stopped');
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

// Export the classes
module.exports = {
  ImprovedWorker,
  ImprovedWorkerPool
};

// If this file is run directly, start a worker
if (require.main === module) {
  const config = {
    REDIS_HOST: process.env.REDIS_HOST || 'localhost',
    REDIS_PORT: parseInt(process.env.REDIS_PORT || '6379'),
    WORKER_ID: process.env.WORKER_ID || uuidv4()
  };

  const worker = new ImprovedWorker(config);

  // Handle graceful shutdown
  process.on('SIGTERM', async () => {
    console.log(`Worker ${config.WORKER_ID} received SIGTERM, shutting down gracefully`);
    await worker.stop();
    process.exit(0);
  });

  process.on('SIGINT', async () => {
    console.log(`Worker ${config.WORKER_ID} received SIGINT, shutting down gracefully`);
    await worker.stop();
    process.exit(0);
  });

  // Start the worker
  worker.start().catch(error => {
    console.error(`Error starting worker:`, error);
    process.exit(1);
  });
}
