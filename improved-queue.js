/**
 * Improved Request Queue System for Marine Traffic Scraper
 *
 * This module implements a more efficient queue system for handling scraping requests.
 * It uses Redis for task distribution, allowing workers to pull tasks when they're available.
 */

const Redis = require('ioredis');

class ImprovedQueue {
  constructor(config) {
    this.config = config;

    // Create Redis clients
    this.redis = new Redis({
      host: config.REDIS_HOST,
      port: config.REDIS_PORT
    });

    // Queue names
    this.taskQueueKey = 'mt:tasks:queue';
    this.processingSetKey = 'mt:tasks:processing';
    this.resultsKey = 'mt:results';

    console.log('Improved queue system initialized');
  }

  /**
   * Add a task to the queue
   * @param {Object} task - The task to add
   * @param {number} priority - Priority of the task (higher is more important)
   * @returns {Promise<string>} - Promise that resolves with the task ID
   */
  async addTask(task, priority = 0) {
    const taskId = task.taskId;

    // Store task details in Redis
    const taskKey = `mt:task:${taskId}`;
    await this.redis.hmset(taskKey, {
      'taskId': taskId,
      'type': task.type,
      'z': task.z,
      'x': task.x,
      'y': task.y,
      'priority': priority,
      'timestamp': Date.now(),
      'status': 'queued'
    });

    // Set expiration on task details (1 hour)
    await this.redis.expire(taskKey, 3600);

    // Add to sorted set with priority as score (higher priority = higher score)
    await this.redis.zadd(this.taskQueueKey, priority, taskId);

    console.log(`Task ${taskId} added to queue with priority ${priority}`);

    return taskId;
  }

  /**
   * Get the next available task from the queue
   * @returns {Promise<Object|null>} - Promise that resolves with the task or null if none available
   */
  async getNextTask() {
    // Use Redis transaction to ensure atomicity
    const result = await this.redis.multi()
      // Get the highest priority task (highest score)
      .zrevrange(this.taskQueueKey, 0, 0)
      // Execute the transaction
      .exec();

    // Check if we got a task
    if (!result || !result[0] || !result[0][1] || result[0][1].length === 0) {
      return null;
    }

    const taskId = result[0][1][0];

    // Move task from queue to processing set
    const moved = await this.redis.multi()
      .zrem(this.taskQueueKey, taskId)
      .sadd(this.processingSetKey, taskId)
      .exec();

    // If task was already taken by another worker
    if (!moved || !moved[0] || !moved[0][1]) {
      return null;
    }

    // Get task details
    const taskKey = `mt:task:${taskId}`;
    const taskDetails = await this.redis.hgetall(taskKey);

    if (!taskDetails) {
      // Task details expired or missing, remove from processing
      await this.redis.srem(this.processingSetKey, taskId);
      return null;
    }

    // Update task status
    await this.redis.hset(taskKey, 'status', 'processing');

    return {
      taskId,
      type: taskDetails.type,
      z: parseInt(taskDetails.z),
      x: parseInt(taskDetails.x),
      y: parseInt(taskDetails.y),
      priority: parseInt(taskDetails.priority),
      timestamp: parseInt(taskDetails.timestamp)
    };
  }

  /**
   * Complete a task with a result
   * @param {string} taskId - The task ID
   * @param {Object} result - The result of the task
   * @param {string} workerId - The worker ID
   * @returns {Promise<boolean>} - Promise that resolves with success status
   */
  async completeTask(taskId, result, workerId) {
    // Remove from processing set
    await this.redis.srem(this.processingSetKey, taskId);

    // Update task status
    const taskKey = `mt:task:${taskId}`;
    await this.redis.hset(taskKey, 'status', 'completed');

    // Publish result
    await this.redis.publish(this.resultsKey, JSON.stringify({
      taskId,
      workerId,
      status: 'completed',
      result,
      timestamp: Date.now()
    }));

    console.log(`Task ${taskId} completed by worker ${workerId}`);

    return true;
  }

  /**
   * Fail a task with an error
   * @param {string} taskId - The task ID
   * @param {Error} error - The error that occurred
   * @param {string} workerId - The worker ID
   * @param {boolean} retry - Whether to retry the task
   * @param {number} maxRetries - Maximum number of retries
   * @returns {Promise<boolean>} - Promise that resolves with success status
   */
  async failTask(taskId, error, workerId, retry = true, maxRetries = 3) {
    // Remove from processing set
    await this.redis.srem(this.processingSetKey, taskId);

    // Get current retry count
    const taskKey = `mt:task:${taskId}`;
    const retryCount = parseInt(await this.redis.hget(taskKey, 'retryCount') || '0');

    // Check if we should retry
    if (retry && retryCount < maxRetries) {
      // Update retry count and status
      await this.redis.hmset(taskKey, {
        'status': 'queued',
        'retryCount': retryCount + 1
      });

      // Get task details
      const taskDetails = await this.redis.hgetall(taskKey);

      // Add back to queue with increased priority
      const priority = parseInt(taskDetails.priority) + 1;
      await this.redis.zadd(this.taskQueueKey, priority, taskId);

      console.log(`Task ${taskId} failed, retrying (${retryCount + 1}/${maxRetries})`);
    } else {
      // Update task status
      await this.redis.hset(taskKey, 'status', 'failed');

      // Publish failure
      await this.redis.publish(this.resultsKey, JSON.stringify({
        taskId,
        workerId,
        status: 'failed',
        error: error.message,
        timestamp: Date.now()
      }));

      console.log(`Task ${taskId} failed permanently: ${error.message}`);
    }

    return true;
  }

  /**
   * Get the number of tasks in the queue
   * @returns {Promise<number>} - Promise that resolves with the queue length
   */
  async getQueueLength() {
    return await this.redis.zcard(this.taskQueueKey);
  }

  /**
   * Get the number of tasks being processed
   * @returns {Promise<number>} - Promise that resolves with the processing count
   */
  async getProcessingCount() {
    return await this.redis.scard(this.processingSetKey);
  }

  /**
   * Clean up stale processing tasks (tasks that have been processing for too long)
   * @param {number} maxProcessingTime - Maximum processing time in milliseconds
   * @returns {Promise<number>} - Promise that resolves with the number of tasks requeued
   */
  async cleanupStaleTasks(maxProcessingTime = 60000) {
    // Get all processing tasks
    const processingTasks = await this.redis.smembers(this.processingSetKey);
    let requeuedCount = 0;

    for (const taskId of processingTasks) {
      const taskKey = `mt:task:${taskId}`;
      const taskDetails = await this.redis.hgetall(taskKey);

      if (!taskDetails) {
        // Task details expired, remove from processing
        await this.redis.srem(this.processingSetKey, taskId);
        continue;
      }

      // Check if task has been processing for too long
      const processingTime = Date.now() - parseInt(taskDetails.timestamp);
      if (processingTime > maxProcessingTime) {
        // Remove from processing set
        await this.redis.srem(this.processingSetKey, taskId);

        // Get current retry count
        const retryCount = parseInt(taskDetails.retryCount || '0');

        // Check if we should retry
        if (retryCount < 3) {
          // Update retry count and status
          await this.redis.hmset(taskKey, {
            'status': 'queued',
            'retryCount': retryCount + 1
          });

          // Add back to queue with increased priority
          const priority = parseInt(taskDetails.priority) + 1;
          await this.redis.zadd(this.taskQueueKey, priority, taskId);

          console.log(`Stale task ${taskId} requeued (${retryCount + 1}/3)`);
          requeuedCount++;
        } else {
          // Update task status
          await this.redis.hset(taskKey, 'status', 'failed');

          // Publish failure
          await this.redis.publish(this.resultsKey, JSON.stringify({
            taskId,
            status: 'failed',
            error: 'Task processing timed out',
            timestamp: Date.now()
          }));

          console.log(`Stale task ${taskId} failed permanently`);
        }
      }
    }

    return requeuedCount;
  }

  /**
   * Close the queue connections
   * @returns {Promise<void>}
   */
  async close() {
    await this.redis.quit();
  }
}

module.exports = ImprovedQueue;
