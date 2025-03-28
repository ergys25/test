// process-data.js

/**
 * Determines the ship group based on ship type or MMSI.
 * If shipType is a valid number, it's used directly.
 * If shipType is invalid and MMSI starts with '99', group is 1.
 * Otherwise, the default group is 0.
 *
 * @param {string|number} mmsi - The vessel's MMSI.
 * @param {string|number} shipType - The vessel's ship type.
 * @returns {number} The determined ship group.
 */
function determineShipGroup(mmsi, shipType) {
  // Convert shipType to number
  const shipTypeNum = parseInt(shipType);

  // If shipType is a valid number, return it as the shipgroup
  if (!isNaN(shipTypeNum)) {
    return shipTypeNum;
  }

  // If shipType is not a valid number, check MMSI
  // Ensure mmsi is treated as a string for startsWith check
  if (mmsi && mmsi.toString().startsWith('99')) {
    return 1;
  }

  // Default
  return 0;
}

/**
 * Extracts and transforms vessel data from the raw scraped data array.
 *
 * @param {Array} rawData - The array of captured request/response objects from the scraper.
 * @param {number} tileX - The X coordinate of the tile (used as fallback).
 * @param {number} tileY - The Y coordinate of the tile (used as fallback).
 * @returns {Array} An array of formatted vessel data objects.
 */
function processScrapedData(rawData, tileX, tileY) {
  const vesselsData = [];

  if (!Array.isArray(rawData)) {
    console.error('Invalid rawData input to processScrapedData: Expected an array.');
    return vesselsData; // Return empty array if input is invalid
  }

  rawData.forEach(item => {
    // Ensure item and nested properties exist
    if (item && item.response && item.response.data) {
      const responseData = item.response.data;

      // Check if the response structure matches expected vessel data format
      if (responseData.type === 1 && responseData.data && Array.isArray(responseData.data.rows)) {
        responseData.data.rows.forEach(vessel => {
          // Basic validation for essential fields like SHIP_ID
          if (!vessel || typeof vessel !== 'object' || !vessel.SHIP_ID) {
            console.warn('Skipping invalid vessel entry:', vessel);
            return; // Skip this vessel entry if essential data is missing
          }

          const currentTime = Date.now();
          // Ensure ELAPSED is treated safely, default to '0' if null/undefined
          const elapsedMs = parseInt(vessel.ELAPSED || '0') * 1000;
          const timestamp = (currentTime - elapsedMs).toString();

          // Safely parse numbers, providing defaults or null
          const mmsi = vessel.SHIP_ID ? parseInt(vessel.SHIP_ID) : null;
          const speed = vessel.SPEED ? parseFloat(vessel.SPEED) / 10 : 0; // Default to 0 if SPEED is missing/invalid
          const cog = vessel.COURSE ? parseFloat(vessel.COURSE) : null;
          const heading = vessel.HEADING ? parseFloat(vessel.HEADING) : null;
          const lat = vessel.LAT ? parseFloat(vessel.LAT) : null;
          const lng = vessel.LON ? parseFloat(vessel.LON) : null;
          const shiptype = vessel.SHIPTYPE ? parseInt(vessel.SHIPTYPE) : null;

          // Calculate dimensions safely
          const l_fore = vessel.L_FORE ? parseInt(vessel.L_FORE) : null;
          const length = vessel.LENGTH ? parseInt(vessel.LENGTH) : null;
          const w_left = vessel.W_LEFT ? parseInt(vessel.W_LEFT) : null;
          const width = vessel.WIDTH ? parseInt(vessel.WIDTH) : null;

          const a = l_fore;
          const b = length !== null && l_fore !== null ? length - l_fore : null;
          const c = w_left;
          const d = width !== null && w_left !== null ? width - w_left : null;

          // Extract tile info from URL if available, otherwise use fallback
          const requestUrl = item.request?.url || '';
          const tileXMatch = requestUrl.match(/X:(\d+)/);
          const tileYMatch = requestUrl.match(/Y:(\d+)/);

          const transformedVessel = {
            mmsi: mmsi,
            timestamp: timestamp,
            speed: speed,
            cog: cog,
            heading: heading,
            lat: lat,
            lng: lng,
            a: a,
            b: b,
            c: c,
            d: d,
            reqts: currentTime.toString(), // Timestamp of the request processing
            shiptype: shiptype,
            shipgroup: determineShipGroup(mmsi, shiptype), // Use parsed mmsi and shiptype
            iso2: vessel.FLAG ? vessel.FLAG.toLowerCase() : null,
            country: null, // Placeholder, original code didn't populate this
            name: vessel.SHIPNAME || null,
            destination: vessel.DESTINATION || null,
            _tileX: tileXMatch ? tileXMatch[1] : tileX.toString(),
            _tileY: tileYMatch ? tileYMatch[1] : tileY.toString()
          };

          // Only add vessel if MMSI and coordinates are valid
          if (transformedVessel.mmsi !== null && transformedVessel.lat !== null && transformedVessel.lng !== null) {
             vesselsData.push(transformedVessel);
          } else {
             console.warn('Skipping vessel due to missing MMSI or coordinates:', vessel.SHIP_ID);
          }
        });
      }
    } else {
       console.warn('Skipping item due to missing response or data structure:', item?.request?.url);
    }
  });

  return vesselsData;
}

// Export the primary processing function
module.exports = { processScrapedData };
