/**
 * Drip Worker for sengine-workers
 * Background worker that processes pending drip messages
 *
 * Run standalone: node workers/dripWorker.js
 * Or as part of main app: require('./workers/dripWorker').start()
 *
 * @module workers/dripWorker
 */

const path = require('path');

// Load environment variables
require('dotenv').config({ path: path.join(__dirname, '../.env') });

const { logger } = require('../services/logger.service');
const dripScheduler = require('../services/drip/dripScheduler.service');
const CONFIG = require('../config/config');

// Configuration
const BATCH_SIZE = CONFIG.DRIP_WORKER.BATCH_SIZE;
const INTERVAL_MS = CONFIG.DRIP_WORKER.INTERVAL_MS;

let isRunning = false;
let intervalId = null;

/**
 * Process a batch of pending drips
 */
const processDrips = async () => {
    if (isRunning) {
        console.log('[DripWorker] Previous batch still running, skipping...');
        return;
    }

    isRunning = true;
    const startTime = Date.now();

    console.log('[DripWorker] ========================================');
    console.log('[DripWorker] Starting drip processing batch');
    console.log('[DripWorker] Time:', new Date().toISOString());
    console.log('[DripWorker] ========================================');

    try {
        const results = await dripScheduler.processPendingDrips(BATCH_SIZE);

        const duration = Date.now() - startTime;

        console.log('[DripWorker] Batch complete:');
        console.log(`  - Processed: ${results.processed}`);
        console.log(`  - Sent: ${results.sent}`);
        console.log(`  - Failed: ${results.failed}`);
        console.log(`  - Skipped: ${results.skipped}`);
        console.log(`  - Duration: ${duration}ms`);

        if (results.errors.length > 0) {
            console.log('[DripWorker] Errors:', JSON.stringify(results.errors.slice(0, 5), null, 2));
            logger.error('[DripWorker] Batch errors:', results.errors);
        }

    } catch (error) {
        console.error('[DripWorker] Error processing drips:', error);
        logger.error('[DripWorker] Error processing drips:', error);
    } finally {
        isRunning = false;
    }
};

/**
 * Start the drip worker
 */
const start = () => {
    console.log('[DripWorker] ========================================');
    console.log('[DripWorker] Starting Drip Worker');
    console.log('[DripWorker] Batch Size:', BATCH_SIZE);
    console.log('[DripWorker] Interval:', INTERVAL_MS, 'ms');
    console.log('[DripWorker] ========================================');

    // Process immediately on start
    processDrips();

    // Then process on interval
    intervalId = setInterval(processDrips, INTERVAL_MS);

    console.log('[DripWorker] Worker started successfully');
};

/**
 * Stop the drip worker
 */
const stop = () => {
    console.log('[DripWorker] Stopping worker...');

    if (intervalId) {
        clearInterval(intervalId);
        intervalId = null;
    }

    console.log('[DripWorker] Worker stopped');
};

/**
 * Run once (for testing or manual execution)
 */
const runOnce = async () => {
    console.log('[DripWorker] Running single batch...');
    await processDrips();
    console.log('[DripWorker] Single batch complete');
};

// Export for use in main app
module.exports = {
    start,
    stop,
    runOnce,
    processDrips
};

// If running as standalone script
if (require.main === module) {
    console.log('[DripWorker] Running as standalone process');

    // Handle graceful shutdown
    process.on('SIGINT', () => {
        console.log('\n[DripWorker] Received SIGINT, shutting down...');
        stop();
        process.exit(0);
    });

    process.on('SIGTERM', () => {
        console.log('\n[DripWorker] Received SIGTERM, shutting down...');
        stop();
        process.exit(0);
    });

    // Start the worker
    start();
}
