// scraper-worker.js - Worker service for handling scraping tasks
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const { scrapeQueue, resultsQueue } = require('./queue.js');
const { scrapeTile } = require('./scraper.js');

// Apply stealth plugin
puppeteer.use(StealthPlugin());

// Configuration
const config = {
  // Scraping Configuration
  SCRAPE_TIMEOUT: parseInt(process.env.SCRAPE_TIMEOUT || '15000'), // 15 seconds default timeout per tile
  MAX_CONCURRENT_JOBS: parseInt(process.env.MAX_CONCURRENT_JOBS || '10'), // Maximum concurrent scraping jobs
  BROWSER_RESTART_INTERVAL: parseInt(process.env.BROWSER_RESTART_INTERVAL || '3600000'), // Restart browser every hour
  PAGE_REUSE_COUNT: parseInt(process.env.PAGE_REUSE_COUNT || '20'), // Reuse pages for this many requests before recycling
  // Memory Management
  MEMORY_MONITOR_INTERVAL: parseInt(process.env.MEMORY_MONITOR_INTERVAL || '300000'), // Check memory usage every 5 minutes
  MEMORY_LIMIT_MB: parseInt(process.env.MEMORY_LIMIT_MB || '1000'), // Restart browser if memory exceeds this limit (MB)
  // Puppeteer Launch Arguments
  PUPPETEER_ARGS: [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-infobars',
    '--window-position=0,0',
    '--ignore-certificate-errors',
    '--disable-dev-shm-usage',
    '--disable-accelerated-2d-canvas',
    '--no-first-run',
    '--no-zygote',
    '--disable-gpu',
    '--disable-extensions',
    '--disable-background-networking',
    '--disable-default-apps',
    '--disable-sync',
    '--disable-translate',
    '--hide-scrollbars',
    '--metrics-recording-only',
    '--mute-audio',
    '--no-experiments',
    '--safebrowsing-disable-auto-update',
    '--js-flags=--expose-gc,--max-old-space-size=500',
    '--disable-notifications',
    '--disable-webgl',
    '--blink-settings=imagesEnabled=false'
  ]
};

// Global state
let browserInstance = null;
let isBrowserReady = false;
let isShuttingDown = false;
let browserStartTime = 0;

// Initialize browser
async function initializeBrowser() {
  if (browserInstance) return; // Already initialized

  console.log('Initializing Puppeteer browser instance...');
  try {
    browserInstance = await puppeteer.launch({
      headless: 'new',
      args: config.PUPPETEER_ARGS
    });

    isBrowserReady = true;
    browserStartTime = Date.now();
    console.log('Puppeteer browser instance initialized successfully.');

    browserInstance.on('disconnected', () => {
      console.error('Puppeteer browser disconnected unexpectedly!');
      isBrowserReady = false;
      browserInstance = null;

      if (!isShuttingDown) {
        console.log('Attempting to relaunch browser...');
        initializeBrowser().catch(err => console.error('Failed to relaunch browser:', err));
      }
    });

  } catch (error) {
    console.error('Failed to initialize Puppeteer browser:', error);
    isBrowserReady = false;
    browserInstance = null;
  }
}

// Check if browser needs to be restarted
async function checkBrowserHealth() {
  // If browser is not ready, initialize it
  if (!isBrowserReady || !browserInstance) {
    await initializeBrowser();
    return;
  }

  // Check if browser needs to be restarted based on uptime
  const uptime = Date.now() - browserStartTime;
  if (uptime > config.BROWSER_RESTART_INTERVAL) {
    console.log(`Browser uptime ${uptime}ms exceeds restart interval. Restarting browser...`);
    try {
      await browserInstance.close();
    } catch (error) {
      console.error('Error closing browser for restart:', error);
    }

    browserInstance = null;
    isBrowserReady = false;
    await initializeBrowser();
  }
}

// Process a scraping job
async function processScrapeJob(job) {
  const { z, x, y } = job.data;
  console.log(`Processing scrape job: z=${z}, x=${x}, y=${y}, job ID: ${job.id}`);

  // Check browser health before scraping
  await checkBrowserHealth();

  // If browser is still not ready, fail the job
  if (!isBrowserReady || !browserInstance) {
    console.error(`Browser not ready for job ${job.id}, z=${z}, x=${x}, y=${y}`);
    throw new Error('Browser instance is not available.');
  }

  try {
    // Perform the scraping
    console.log(`Starting scrape for job ${job.id}, z=${z}, x=${x}, y=${y}`);
    const rawData = await scrapeTile(browserInstance, z, x, y, config.SCRAPE_TIMEOUT);
    console.log(`Scrape completed for job ${job.id}, captured ${rawData.length} data points`);

    // Process the data directly here instead of using the results queue
    console.log(`Processing data for job ${job.id}`);
    const { processScrapedData } = require('./process-data.js');
    const vesselsData = processScrapedData(rawData, x, y);
    console.log(`Processed ${vesselsData.length} vessels for job ${job.id}`);

    // Also add to results queue for caching and background processing
    console.log(`Adding results to queue for job ${job.id}`);
    const resultJob = await resultsQueue.add({
      z, x, y,
      rawData,
      timestamp: Date.now()
    });

    console.log(`Results queued successfully for job ${job.id}, result job ID: ${resultJob.id}`);

    // Return the actual vessel data instead of just metadata
    return vesselsData;
  } catch (error) {
    console.error(`Error scraping tile z=${z}, x=${x}, y=${y}, job ID: ${job.id}:`, error);
    throw error; // Let Bull handle retries
  }
}

// Start the worker
async function startWorker() {
  console.log(`Starting scraper worker with concurrency: ${config.MAX_CONCURRENT_JOBS}`);

  // Initialize browser
  await initializeBrowser();

  // Process jobs from the queue
  scrapeQueue.process(config.MAX_CONCURRENT_JOBS, processScrapeJob);

  console.log('Scraper worker started successfully.');

  // Set up graceful shutdown
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

// Graceful shutdown
async function shutdown() {
  if (isShuttingDown) return;
  isShuttingDown = true;

  console.log('Shutting down scraper worker...');

  // Stop processing new jobs
  await scrapeQueue.pause();

  // Close browser
  if (browserInstance) {
    try {
      await browserInstance.close();
      console.log('Browser closed successfully.');
    } catch (error) {
      console.error('Error closing browser:', error);
    }
  }

  // Close queues
  await scrapeQueue.close();
  await resultsQueue.close();

  console.log('Scraper worker shutdown complete.');
  process.exit(0);
}

// Start the worker if this is the main module
if (require.main === module) {
  startWorker().catch(error => {
    console.error('Failed to start scraper worker:', error);
    process.exit(1);
  });
}

module.exports = {
  startWorker,
  shutdown
};
