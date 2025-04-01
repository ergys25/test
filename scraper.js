const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');

// Add stealth plugin to puppeteer
puppeteer.use(StealthPlugin());

// Browser pool for reusing browser instances
class BrowserPool {
  constructor(maxSize = 1) { // Reduced to 1 browser per worker
    this.maxSize = maxSize;
    this.browsers = [];
    this.inUse = new Set();
    this.lastUsed = new Map();
    this.pages = new Map(); // Map to store preloaded pages for each browser
    this.pageInUse = new Map(); // Track which pages are in use
  }

  async getBrowser() {
    // Check if there's an available browser in the pool
    const availableBrowser = this.browsers.find(browser => !this.inUse.has(browser));

    if (availableBrowser) {
      this.inUse.add(availableBrowser);
      this.lastUsed.set(availableBrowser, Date.now());
      return availableBrowser;
    }

    // If pool is not full, create a new browser
    if (this.browsers.length < this.maxSize) {
      const newBrowser = await this.createBrowser();
      this.browsers.push(newBrowser);
      this.inUse.add(newBrowser);
      this.lastUsed.set(newBrowser, Date.now());

      // Preload pages for this browser
      await this.preloadPages(newBrowser);

      return newBrowser;
    }

    // If pool is full, wait for a browser to become available
    return new Promise(resolve => {
      const checkInterval = setInterval(() => {
        const availableBrowser = this.browsers.find(browser => !this.inUse.has(browser));
        if (availableBrowser) {
          clearInterval(checkInterval);
          this.inUse.add(availableBrowser);
          this.lastUsed.set(availableBrowser, Date.now());
          resolve(availableBrowser);
        }
      }, 50); // Reduced interval for faster checking
    });
  }

  // Preload pages for a browser
  async preloadPages(browser, pageCount = 1) { // Preload 1 page per browser
    if (!this.pages.has(browser)) {
      this.pages.set(browser, []);
      this.pageInUse.set(browser, new Set());
    }

    const pages = this.pages.get(browser);
    const pagesInUse = this.pageInUse.get(browser);

    // Create pages up to the desired count
    while (pages.length < pageCount) {
      try {
        const page = await browser.newPage();

        // Set common headers
        await page.setExtraHTTPHeaders({
          'Accept-Language': 'en-US,en;q=0.9',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
          'Accept-Encoding': 'gzip, deflate, br'
        });

        // Set up request interception for XHR only
        await page.setRequestInterception(true);

        pages.push(page);
        console.log(`Preloaded page ${pages.length} for browser`);
      } catch (error) {
        console.error('Error preloading page:', error);
        break;
      }
    }
  }

  // Get an available page from a browser
  async getPage(browser) {
    if (!this.pages.has(browser)) {
      await this.preloadPages(browser);
    }

    const pages = this.pages.get(browser);
    const pagesInUse = this.pageInUse.get(browser);

    // Find an available page
    const availablePage = pages.find(page => !pagesInUse.has(page));
    if (availablePage) {
      pagesInUse.add(availablePage);
      return availablePage;
    }

    // If no available page, create a new one
    try {
      const newPage = await browser.newPage();

      // Set common headers
      await newPage.setExtraHTTPHeaders({
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br'
      });

      // Set up request interception for XHR only
      await newPage.setRequestInterception(true);

      pages.push(newPage);
      pagesInUse.add(newPage);
      return newPage;
    } catch (error) {
      console.error('Error creating new page:', error);
      throw error;
    }
  }

  // Release a page back to the pool
  releasePage(browser, page) {
    if (this.pages.has(browser) && this.pageInUse.has(browser)) {
      const pagesInUse = this.pageInUse.get(browser);
      if (pagesInUse.has(page)) {
        pagesInUse.delete(page);
      }
    }
  }

  async createBrowser() {
    // Get additional args from environment variable
    const puppeteerArgs = process.env.PUPPETEER_ARGS ? process.env.PUPPETEER_ARGS.split(' ') : [];

    // Combine with default args
    const args = [
      '--disable-infobars',
      '--window-position=0,0',
      '--ignore-certificate-errors',
      '--ignore-certificate-errors-spki-list',
      '--disable-features=IsolateOrigins,site-per-process',
      '--disable-site-isolation-trials',
      '--disable-web-security',
      '--disable-features=site-per-process',
      ...puppeteerArgs
    ];

    console.log('Launching new browser with args:', args);

    return await puppeteer.launch({
      headless: 'new', // Use headless mode in Docker
      args,
      ignoreHTTPSErrors: true,
      defaultViewport: {
        width: 1920,
        height: 1080
      }
    });
  }

  releaseBrowser(browser) {
    if (this.inUse.has(browser)) {
      this.inUse.delete(browser);
      this.lastUsed.set(browser, Date.now());
    }
  }

  async closeBrowser(browser) {
    if (this.browsers.includes(browser)) {
      const index = this.browsers.indexOf(browser);
      if (index > -1) {
        this.browsers.splice(index, 1);
      }
      this.inUse.delete(browser);
      this.lastUsed.delete(browser);

      try {
        await browser.close();
      } catch (error) {
        console.error('Error closing browser:', error);
      }
    }
  }

  async cleanup(maxIdleTime = 300000) { // 5 minutes by default
    const now = Date.now();

    for (const browser of this.browsers) {
      if (!this.inUse.has(browser)) {
        const lastUsedTime = this.lastUsed.get(browser) || 0;
        if (now - lastUsedTime > maxIdleTime) {
          await this.closeBrowser(browser);
        }
      }
    }
  }

  async closeAll() {
    for (const browser of this.browsers) {
      try {
        await browser.close();
      } catch (error) {
        console.error('Error closing browser during pool shutdown:', error);
      }
    }

    this.browsers = [];
    this.inUse.clear();
    this.lastUsed.clear();
  }
}

// Create a global browser pool instance
const browserPool = new BrowserPool(1); // One browser per worker

// Start a cleanup interval to close idle browsers
setInterval(() => browserPool.cleanup(), 300000); // Check every 5 minutes

// Retry configuration
const MAX_RETRIES = 1;
const RETRY_DELAY = 2; // 5 seconds

// Function to delay execution
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// Retry wrapper function
async function withRetry(operation, retries = MAX_RETRIES) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      if (attempt === retries) {
        console.error(`Failed after ${retries} attempts:`, error);
        return null; // Return empty result instead of throwing
      }
      console.log(`Attempt ${attempt} failed, retrying in ${RETRY_DELAY/1000} seconds...`);
      await delay(RETRY_DELAY);
    }
  }
}

// Default tile coordinates if not provided (only used when running directly)
const DEFAULT_ZOOM = 7;
const DEFAULT_X = 73;
const DEFAULT_Y = 50;

/**
 * Scrape Marine Traffic for a specific tile
 * @param {number} z - Zoom level
 * @param {number} x - X coordinate
 * @param {number} y - Y coordinate
 */
async function scrapeMarineTraffic(z, x, y) {
  let browser = null;
  let page = null;
  const startTime = Date.now();

  try {
    // Only log when actually scraping
    if (z && x && y) {
      console.log(`Targeting tile: z=${z}, x=${x}, y=${y}`);
    }

    // Get a browser from the pool instead of launching a new one
    browser = await browserPool.getBrowser();
    console.log(`Using browser from pool for tile z=${z}, x=${x}, y=${y}`);

    // Get a preloaded page from the browser pool
    page = await browserPool.getPage(browser);
    console.log(`Got preloaded page for tile z=${z}, x=${x}, y=${y}`);

    if (!page) {
      console.error('Failed to get page');
      return { tileInfo: { z, x, y }, requestCount: 0, outputDir: null };
    }

    // Reset page event listeners
    await page.removeAllListeners('request');
    await page.removeAllListeners('response');

    // Clear any previous navigation data - wrapped in try/catch to handle security restrictions
    try {
      await page.evaluate(() => {
        try {
          localStorage.clear();
          sessionStorage.clear();
        } catch (e) {
          // Ignore storage access errors - this is expected in some security contexts
          console.log('Storage access restricted, continuing without clearing');
        }
      });
    } catch (e) {
      // Ignore evaluation errors
      console.log('Storage clearing skipped due to security restrictions');
    }

    // Array to store matching XHR requests and responses
    const matchingRequests = [];

    // Set up optimized request interception - only intercept XHR
    await page.setRequestInterception(true);

    // Listen for XHR requests
    page.on('request', request => {
      const url = request.url();
      const resourceType = request.resourceType();

      // Only process XHR requests and abort unnecessary resources to speed up loading
      if (resourceType === 'image' || resourceType === 'font' || resourceType === 'media') {
        request.abort();
        return;
      } else if (resourceType !== 'xhr') {
        request.continue();
        return;
      }

      // Check for various station patterns
      const stationPatterns = [
        '/station:0/?cb=',
        'station:0/?cb=',
        '/station:0?cb=',
        'station:0?cb=',
        '/station:',
        'station:'
      ];

      const matchesPattern = stationPatterns.some(pattern => url.includes(pattern));

      if (matchesPattern) {
        // Store request details
        const requestDetails = {
          url: url,
          method: request.method(),
          headers: request.headers(),
          postData: request.postData(),
          timestamp: new Date().toISOString()
        };

        // Add to our collection with placeholder for response
        matchingRequests.push({
          request: requestDetails,
          response: null
        });
      }

      // Continue the request
      request.continue();
    });

    // Listen for responses - check for various station patterns
    page.on('response', async response => {
      const url = response.url();

      // Check for various station patterns
      const stationPatterns = [
        '/station:0/?cb=',
        'station:0/?cb=',
        '/station:0?cb=',
        'station:0?cb=',
        '/station:',
        'station:'
      ];

      const matchesPattern = stationPatterns.some(pattern => url.includes(pattern));

      if (matchesPattern) {
        try {
          // Find the matching request in our collection
          const matchingRequest = matchingRequests.find(item => item.request.url === url && item.response === null);

          if (matchingRequest) {
            // Try to get response data
            let responseData;
            try {
              responseData = await response.json();
            } catch (e) {
              try {
                responseData = await response.text();
              } catch (e) {
                responseData = 'Could not parse response';
              }
            }

            // Add response details
            matchingRequest.response = {
              status: response.status(),
              headers: await response.headers(),
              data: responseData
            };
          }
        } catch (error) {
          console.error(`Error processing response for ${url}:`, error);
        }
      }
    });

    // Calculate coordinates
    const lon = (x / Math.pow(2, z) * 360 - 180).toFixed(6);
    const lat = ((Math.atan(Math.sinh(Math.PI * (1 - 2 * y / Math.pow(2, z)))) * 180) / Math.PI).toFixed(6);
    const url = `https://www.marinetraffic.com/en/ais/home/centerx:${lon}/centery:${lat}/zoom:${z+1}`;

    console.log(`Navigating to Marine Traffic for tile z=${z}, x=${x}, y=${y}...`);

    // Use a more aggressive navigation strategy with better error handling
    const navigationSuccess = await withRetry(async () => {
      try {
        // First try with minimal wait to speed up the process
        console.log(`Attempting fast navigation to ${url}`);
        await page.goto(url, {
          waitUntil: 'domcontentloaded',
          timeout: 30000 // Reduced timeout for faster failure detection
        });

        // Check if we got the page by looking for common elements
        const hasMap = await page.evaluate(() => {
          return !!document.querySelector('#map') ||
                 !!document.querySelector('.leaflet-container') ||
                 !!document.querySelector('.mapboxgl-canvas-container');
        }).catch(() => false);

        if (hasMap) {
          console.log('Map element found, navigation successful');
          return true;
        }

        // If no map element found, try to wait a bit more
        console.log('Map element not found immediately, waiting a bit longer...');
        await page.waitForSelector('#map, .leaflet-container, .mapboxgl-canvas-container', {
          timeout: 10000
        }).catch(() => {});

        // Check again if we have the map
        const hasMapAfterWait = await page.evaluate(() => {
          return !!document.querySelector('#map') ||
                 !!document.querySelector('.leaflet-container') ||
                 !!document.querySelector('.mapboxgl-canvas-container');
        }).catch(() => false);

        if (hasMapAfterWait) {
          console.log('Map element found after waiting, navigation successful');
          return true;
        }

        // If still no map, check if we have any XHR requests that match our patterns
        if (matchingRequests.length > 0) {
          console.log('No map element found, but matching XHR requests captured, considering navigation successful');
          return true;
        }

        console.log('Navigation completed but no map or matching XHR requests found');
        return false;
      } catch (error) {
        console.error(`Navigation error: ${error.message}`);

        // Even if navigation fails, check if we captured any matching requests
        if (matchingRequests.length > 0) {
          console.log('Navigation failed but matching XHR requests captured, considering navigation successful');
          return true;
        }

        return false;
      }
    }, 2); // Increase retry attempts for navigation

    if (!navigationSuccess) {
      console.error('Failed to navigate to Marine Traffic');
      return { tileInfo: { z, x, y }, requestCount: 0, outputDir: null };
    }

    // Wait a short time for XHR requests to complete
    // Use page.waitForTimeout if available, otherwise use setTimeout
    try {
      if (typeof page.waitForTimeout === 'function') {
        await page.waitForTimeout(2000);
      } else {
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    } catch (error) {
      console.log('Error during wait, using setTimeout fallback:', error);
      await new Promise(resolve => setTimeout(resolve, 2000));
    }

    // Release the page back to the pool instead of closing it
    browserPool.releasePage(browser, page);

    // Release the browser back to the pool
    browserPool.releaseBrowser(browser);

    const endTime = Date.now();
    console.log(`Completed scraping for tile z=${z}, x=${x}, y=${y} in ${endTime - startTime}ms`);

    // Return the captured requests directly
    return matchingRequests;
  } catch (error) {
    console.error('Fatal error during scraping:', error);

    // Clean up resources on error
    try {
      if (page) {
        browserPool.releasePage(browser, page);
      }
      if (browser) {
        browserPool.releaseBrowser(browser);
      }
    } catch (releaseError) {
      console.error('Error releasing resources after fatal error:', releaseError);
    }

    // Return empty result instead of throwing
    return {
      tileInfo: { z, x, y },
      requestCount: 0,
      outputDir: null
    };
  }
}

// Parse command line arguments
function parseArgs() {
  const args = process.argv.slice(2);
  let z = DEFAULT_ZOOM;
  let x = DEFAULT_X;
  let y = DEFAULT_Y;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--z' && i + 1 < args.length) {
      z = parseInt(args[i + 1], 10);
    } else if (args[i] === '--x' && i + 1 < args.length) {
      x = parseInt(args[i + 1], 10);
    } else if (args[i] === '--y' && i + 1 < args.length) {
      y = parseInt(args[i + 1], 10);
    }
  }

  return { z, x, y };
}

// Timeout wrapper function
async function withTimeout(operation, timeoutMs = 9000, params = {}) {
  let timeoutId;
  let browser = null;

  try {
    const result = await Promise.race([
      operation().then(result => {
        if (result && result.browser) {
          browser = result.browser;
        }
        return result;
      }),
      new Promise((_, reject) => {
        timeoutId = setTimeout(() => {
          reject(new Error(`Operation timed out after ${timeoutMs}ms`));
        }, timeoutMs);
      })
    ]);
    clearTimeout(timeoutId);
    return result;
  } catch (error) {
    clearTimeout(timeoutId);
    console.error('Error during operation:', error);

    // We don't need to handle browser cleanup here anymore
    // as the scrapeMarineTraffic function handles releasing browsers back to the pool

    // Use the provided parameters for the error response
    return {
      tileInfo: {
        z: params.z || null,
        x: params.x || null,
        y: params.y || null
      },
      requestCount: 0,
      outputDir: null
    };
  }
}

// Wrap the scraper function to handle timeouts
async function scrapeWithTimeout(z, x, y) {
  return withTimeout(
    async () => {
      return await scrapeMarineTraffic(z, x, y);
    },
    110000, // 110 second timeout (increased from 30 seconds, leaving 10 seconds for task processing)
    { z, x, y } // Pass parameters for error handling
  );
}

// Only run the scraper if this file is executed directly (not imported)
if (require.main === module) {
  const { z, x, y } = parseArgs();
  (async () => {
    try {
      const result = await scrapeWithTimeout(z, x, y);
      console.log('Scraping result:', result);
    } catch (error) {
      console.error('Error running scraper service:', error);
    }
  })();
}

// Export the timeout-wrapped function and browser pool for use as a module
module.exports = {
  scrapeMarineTraffic: scrapeWithTimeout,
  browserPool // Export the browser pool for proper management
};
