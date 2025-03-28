/**
 * Request Queue System for Marine Traffic Scraper
 *
 * This module implements a queue system for handling scraping requests.
 * It manages the distribution of requests to available workers and
 * handles request prioritization and retries.
 */

class Queue {
  constructor(maxConcurrent = 12) {
    this.queue = [];
    this.processing = new Map(); // Map of currently processing requests
    this.workers = new Map(); // Map of worker IDs to their status
    this.maxConcurrent = maxConcurrent;
    this.nextWorkerId = 1;
    this.nextRequestId = 1;
  }

  /**
   * Register a new worker with the queue
   * @returns {number} The worker ID
   */
  registerWorker() {
    const workerId = this.nextWorkerId++;
    this.workers.set(workerId, { status: 'idle', lastActive: Date.now() });
    console.log(`Worker ${workerId} registered with queue`);
    return workerId;
  }

  /**
   * Update a worker's status
   * @param {number} workerId - The worker ID
   * @param {string} status - The new status ('idle' or 'busy')
   */
  updateWorkerStatus(workerId, status) {
    if (!this.workers.has(workerId)) {
      console.warn(`Attempted to update non-existent worker ${workerId}`);
      return;
    }

    this.workers.set(workerId, {
      status,
      lastActive: Date.now()
    });

    // If worker is now idle, try to assign it a new task
    if (status === 'idle') {
      this.processNext();
    }
  }

  /**
   * Add a request to the queue
   * @param {Object} request - The request to add
   * @param {Function} callback - Callback to call with the result
   * @returns {number} The request ID
   */
  addRequest(request, callback) {
    const requestId = this.nextRequestId++;

    this.queue.push({
      id: requestId,
      request,
      callback,
      addedAt: Date.now(),
      priority: request.priority || 0,
      attempts: 0
    });

    // Sort queue by priority (higher first) and then by time added (older first)
    this.queue.sort((a, b) => {
      if (a.priority !== b.priority) {
        return b.priority - a.priority;
      }
      return a.addedAt - b.addedAt;
    });

    console.log(`Request ${requestId} added to queue. Queue length: ${this.queue.length}`);

    // Try to process the queue
    this.processNext();

    return requestId;
  }

  /**
   * Process the next request in the queue if a worker is available
   */
  processNext() {
    if (this.queue.length === 0) {
      return;
    }

    // Find an idle worker
    let availableWorkerId = null;
    for (const [workerId, info] of this.workers.entries()) {
      if (info.status === 'idle') {
        availableWorkerId = workerId;
        break;
      }
    }

    if (availableWorkerId === null) {
      // No workers available
      return;
    }

    // Get the next request from the queue
    const queueItem = this.queue.shift();

    // Mark the worker as busy
    this.updateWorkerStatus(availableWorkerId, 'busy');

    // Add to processing map
    this.processing.set(queueItem.id, {
      workerId: availableWorkerId,
      request: queueItem.request,
      callback: queueItem.callback,
      startedAt: Date.now(),
      attempts: queueItem.attempts + 1
    });

    console.log(`Assigned request ${queueItem.id} to worker ${availableWorkerId}`);

    // Return the request to be processed
    return {
      requestId: queueItem.id,
      workerId: availableWorkerId,
      request: queueItem.request
    };
  }

  /**
   * Complete a request that was being processed
   * @param {number} requestId - The request ID
   * @param {number} workerId - The worker ID
   * @param {*} result - The result of the request
   * @param {Error|null} error - Any error that occurred
   */
  completeRequest(requestId, workerId, result, error = null) {
    if (!this.processing.has(requestId)) {
      console.warn(`Attempted to complete non-existent request ${requestId}`);
      return;
    }

    const processingItem = this.processing.get(requestId);

    // Ensure the worker ID matches
    if (processingItem.workerId !== workerId) {
      console.warn(`Worker ID mismatch for request ${requestId}. Expected ${processingItem.workerId}, got ${workerId}`);
      return;
    }

    // Call the callback with the result
    try {
      processingItem.callback(error, result);
    } catch (callbackError) {
      console.error(`Error in callback for request ${requestId}:`, callbackError);
    }

    // Remove from processing map
    this.processing.delete(requestId);

    // Mark the worker as idle
    this.updateWorkerStatus(workerId, 'idle');

    console.log(`Request ${requestId} completed by worker ${workerId}`);
  }

  /**
   * Handle a failed request
   * @param {number} requestId - The request ID
   * @param {number} workerId - The worker ID
   * @param {Error} error - The error that occurred
   * @param {boolean} retry - Whether to retry the request
   * @param {number} maxRetries - Maximum number of retries
   */
  failRequest(requestId, workerId, error, retry = true, maxRetries = 3) {
    if (!this.processing.has(requestId)) {
      console.warn(`Attempted to fail non-existent request ${requestId}`);
      return;
    }

    const processingItem = this.processing.get(requestId);

    // Ensure the worker ID matches
    if (processingItem.workerId !== workerId) {
      console.warn(`Worker ID mismatch for request ${requestId}. Expected ${processingItem.workerId}, got ${workerId}`);
      return;
    }

    console.log(`Request ${requestId} failed on worker ${workerId}: ${error.message}`);

    // Check if we should retry
    if (retry && processingItem.attempts < maxRetries) {
      console.log(`Retrying request ${requestId} (attempt ${processingItem.attempts + 1} of ${maxRetries})`);

      // Add back to the queue with increased priority
      this.queue.unshift({
        id: requestId,
        request: processingItem.request,
        callback: processingItem.callback,
        addedAt: Date.now(),
        priority: (processingItem.request.priority || 0) + 1,
        attempts: processingItem.attempts
      });
    } else {
      // Call the callback with the error
      try {
        processingItem.callback(error, null);
      } catch (callbackError) {
        console.error(`Error in callback for failed request ${requestId}:`, callbackError);
      }
    }

    // Remove from processing map
    this.processing.delete(requestId);

    // Mark the worker as idle
    this.updateWorkerStatus(workerId, 'idle');
  }

  /**
   * Get the current queue status
   * @returns {Object} The queue status
   */
  getStatus() {
    return {
      queueLength: this.queue.length,
      processing: this.processing.size,
      workers: Array.from(this.workers.entries()).map(([id, info]) => ({
        id,
        status: info.status,
        lastActive: info.lastActive
      }))
    };
  }

  /**
   * Get the next available request for a worker
   * @param {number} workerId - The worker ID
   * @returns {Object|null} The next request or null if none available
   */
  getNextRequest(workerId) {
    // Ensure the worker exists and is idle
    if (!this.workers.has(workerId)) {
      console.warn(`Worker ${workerId} not registered with queue`);
      return null;
    }

    const workerInfo = this.workers.get(workerId);
    if (workerInfo.status !== 'idle') {
      console.warn(`Worker ${workerId} is not idle`);
      return null;
    }

    // Process the next request
    return this.processNext();
  }
}

// Create a singleton instance
const queueInstance = new Queue();

module.exports = queueInstance;
