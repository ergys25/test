const fs = require('fs');
const path = require('path');

/**
 * Function to load the captured requests
 * @param {string} tileDir - Directory containing the tile data (optional)
 * @param {string} fileName - Name of the file to load (default: captured_requests.json)
 * @returns {Array} The loaded data
 */
function loadCapturedData(tileDir = '', fileName = 'captured_requests.json') {
  try {
    // If tileDir is provided, use it, otherwise look in the current directory
    const filePath = tileDir ? path.join(tileDir, fileName) : fileName;

    // Check if the file exists
    if (!fs.existsSync(filePath)) {
      console.error(`File not found: ${filePath}`);
      return [];
    }

    const data = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(data);
  } catch (error) {
    console.error(`Error loading data from ${tileDir}/${fileName}:`, error);
    return [];
  }
}

// Function to extract URLs only
function extractUrls(data) {
  return data.map(item => item.request.url);
}

/**
 * Function to extract unique parameters from URLs
 * @param {Array} data - The data to process
 * @returns {Object} Object containing unique parameters and their values
 */
function extractUniqueParameters(data) {
  const urls = extractUrls(data);
  const allParams = {};

  urls.forEach(url => {
    try {
      // Extract query parameters
      const queryString = url.split('?')[1];
      if (queryString) {
        const params = new URLSearchParams(queryString);
        params.forEach((value, key) => {
          if (!allParams[key]) {
            allParams[key] = new Set();
          }
          allParams[key].add(value);
        });
      }

      // Also extract path parameters from the URL format we're seeing
      // Example: /getData/get_data_json_4/z:4/X:3/Y:3/station:0
      const pathParts = url.split('/');
      for (const part of pathParts) {
        if (part.includes(':')) {
          const [key, value] = part.split(':');
          if (!allParams[key]) {
            allParams[key] = new Set();
          }
          allParams[key].add(value);
        }
      }
    } catch (error) {
      console.error(`Error processing URL ${url}:`, error);
    }
  });

  // Convert Sets to Arrays for easier reading
  const result = {};
  Object.keys(allParams).forEach(key => {
    result[key] = Array.from(allParams[key]);
  });

  return result;
}

/**
 * Function to export data to CSV
 * @param {Array} data - The data to export
 * @param {string} tileDir - Directory to save the CSV file (optional)
 * @param {string} outputFileName - Name of the output file (default: requests.csv)
 */
function exportToCsv(data, tileDir = '', outputFileName = 'requests.csv') {
  try {
    // If tileDir is provided, use it, otherwise save in the current directory
    const outputPath = tileDir ? path.join(tileDir, outputFileName) : outputFileName;

    // Create CSV header
    let csvContent = 'URL,Method,Status,Timestamp\n';

    // Add each request as a row
    data.forEach(item => {
      const url = item.request.url.replace(/,/g, '%2C'); // Escape commas
      const method = item.request.method;
      const status = item.response?.status || 'N/A';
      const timestamp = item.request.timestamp;

      csvContent += `${url},${method},${status},${timestamp}\n`;
    });

    // Write to file
    fs.writeFileSync(outputPath, csvContent);
    console.log(`Data exported to ${outputPath}`);
  } catch (error) {
    console.error(`Error exporting to CSV:`, error);
  }
}

/**
 * Function to find the most recent tile directory
 * @returns {string|null} Path to the most recent tile directory or null if none found
 */
function findMostRecentTileDir() {
  try {
    const tileDataDir = 'tile_data';

    // Check if tile_data directory exists
    if (!fs.existsSync(tileDataDir)) {
      return null;
    }

    // Get all subdirectories in tile_data
    const tileDirs = fs.readdirSync(tileDataDir, { withFileTypes: true })
      .filter(dirent => dirent.isDirectory())
      .map(dirent => path.join(tileDataDir, dirent.name));

    if (tileDirs.length === 0) {
      return null;
    }

    // Find the most recently modified directory
    let mostRecent = tileDirs[0];
    let mostRecentTime = fs.statSync(mostRecent).mtime.getTime();

    for (let i = 1; i < tileDirs.length; i++) {
      const dirTime = fs.statSync(tileDirs[i]).mtime.getTime();
      if (dirTime > mostRecentTime) {
        mostRecent = tileDirs[i];
        mostRecentTime = dirTime;
      }
    }

    return mostRecent;
  } catch (error) {
    console.error('Error finding most recent tile directory:', error);
    return null;
  }
}

/**
 * Main function to process data
 * @param {string} tileDir - Directory containing the tile data (optional)
 */
function processData(tileDir = '') {
  // If no tileDir provided, try to find the most recent one
  if (!tileDir) {
    tileDir = findMostRecentTileDir();

    if (tileDir) {
      console.log(`Using most recent tile directory: ${tileDir}`);
    } else {
      console.log('No tile directory found, using current directory');
    }
  }

  // Load the data
  const data = loadCapturedData(tileDir);

  if (data.length === 0) {
    console.log('No data found or empty data file.');
    return;
  }

  console.log(`Loaded ${data.length} captured requests.`);

  // Export to CSV
  exportToCsv(data, tileDir);

  // Extract and display unique parameters
  const uniqueParams = extractUniqueParameters(data);
  console.log('\nUnique parameters found in URLs:');
  console.log(JSON.stringify(uniqueParams, null, 2));

  // Save unique parameters to a file
  const uniqueParamsPath = tileDir ? path.join(tileDir, 'unique_parameters.json') : 'unique_parameters.json';
  fs.writeFileSync(uniqueParamsPath, JSON.stringify(uniqueParams, null, 2));
  console.log(`Unique parameters saved to ${uniqueParamsPath}`);
}

// Parse command line arguments
function parseArgs() {
  const args = process.argv.slice(2);
  let tileDir = '';

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--dir' && i + 1 < args.length) {
      tileDir = args[i + 1];
    }
  }

  return { tileDir };
}

// Run the processing
const { tileDir } = parseArgs();
processData(tileDir);
