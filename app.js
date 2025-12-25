/**
 * sengine-workers - Main Entry Point
 *
 * This application runs background workers for:
 * - Message sending (SEND_MESSAGE queue)
 * - Inbound message processing (INBOUND_MESSAGE queue)
 * - Delivery status updates (STATUS_UPDATE queue)
 * - Drip campaign scheduling (cron job)
 *
 * Architecture:
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                      SENGINE-WORKERS                            │
 * │                                                                 │
 * │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
 * │  │   Message   │  │   Inbound   │  │   Status    │             │
 * │  │   Worker    │  │   Worker    │  │   Worker    │             │
 * │  │             │  │             │  │             │             │
 * │  │ Sends SMS   │  │ Processes   │  │ Updates     │             │
 * │  │ via Twilio  │  │ incoming    │  │ delivery    │             │
 * │  └─────────────┘  └─────────────┘  └─────────────┘             │
 * │                                                                 │
 * │  ┌─────────────────────────────────────────────────┐           │
 * │  │              Drip Scheduler                      │           │
 * │  │                                                  │           │
 * │  │  Runs every minute to check what drip messages  │           │
 * │  │  need to be sent NOW and queues them            │           │
 * │  └─────────────────────────────────────────────────┘           │
 * └─────────────────────────────────────────────────────────────────┘
 *
 * Run with: node app.js
 * Or via PM2: pm2 start ecosystem.config.js
 */

const path = require('path');

// Load environment variables
require('dotenv').config({ path: path.join(__dirname, '.env') });

const CONFIG = require('./config/config');
const { logger } = require('./services/logger.service');
const rabbitmq = require('./config/rabbitmq');

// Import workers
const messageWorker = require('./workers/messageWorker');
const inboundWorker = require('./workers/inboundWorker');
const statusWorker = require('./workers/statusWorker');
const dripWorker = require('./workers/dripWorker');

console.log('');
console.log('╔════════════════════════════════════════════════════════════╗');
console.log('║                    SENGINE WORKERS                         ║');
console.log('║              Background Processing Service                 ║');
console.log('╠════════════════════════════════════════════════════════════╣');
console.log('║                                                            ║');
console.log('║  Workers:                                                  ║');
console.log('║  ├── Message Worker    : Send SMS via Twilio              ║');
console.log('║  ├── Inbound Worker    : Process incoming messages        ║');
console.log('║  ├── Status Worker     : Update delivery status           ║');
console.log('║  └── Drip Scheduler    : Schedule drip campaigns          ║');
console.log('║                                                            ║');
console.log('╚════════════════════════════════════════════════════════════╝');
console.log('');
console.log('Configuration:');
console.log('  Environment    :', CONFIG.APP.ENVIRONMENT);
console.log('  Database       :', CONFIG.DB.DATABASE_URL ? 'Connected via URL' : `${CONFIG.DB.HOST}:${CONFIG.DB.PORT}`);
console.log('  RabbitMQ       :', CONFIG.RABBITMQ.ENABLED ? 'Enabled' : 'Disabled');
console.log('');
console.log('Workers Status:');
console.log('  Message Worker :', CONFIG.MESSAGE_WORKER.ENABLED ? '✓ Enabled' : '✗ Disabled');
console.log('  Inbound Worker :', CONFIG.MESSAGE_WORKER.ENABLED ? '✓ Enabled' : '✗ Disabled');
console.log('  Status Worker  :', CONFIG.MESSAGE_WORKER.ENABLED ? '✓ Enabled' : '✗ Disabled');
console.log('  Drip Scheduler :', CONFIG.DRIP_WORKER.ENABLED ? '✓ Enabled' : '✗ Disabled');
console.log('');

/**
 * Start all enabled workers
 */
const startWorkers = async () => {
    try {
        // Connect to RabbitMQ
        if (CONFIG.RABBITMQ.ENABLED) {
            console.log('[App] Connecting to RabbitMQ...');
            await rabbitmq.connect();
            console.log('[App] RabbitMQ connected ✓');
        } else {
            console.error('[App] RabbitMQ is disabled! Workers cannot start.');
            process.exit(1);
        }

        // Give RabbitMQ connection time to stabilize
        await new Promise(resolve => setTimeout(resolve, 2000));

        // Start Message Worker (sends SMS via Twilio)
        if (CONFIG.MESSAGE_WORKER.ENABLED) {
            console.log('[App] Starting Message Worker...');
            await messageWorker.start();
            console.log('[App] Message Worker started ✓');
        }

        // Start Inbound Worker (processes incoming messages)
        if (CONFIG.MESSAGE_WORKER.ENABLED) {
            console.log('[App] Starting Inbound Worker...');
            await inboundWorker.start();
            console.log('[App] Inbound Worker started ✓');
        }

        // Start Status Worker (updates delivery status)
        if (CONFIG.MESSAGE_WORKER.ENABLED) {
            console.log('[App] Starting Status Worker...');
            await statusWorker.start();
            console.log('[App] Status Worker started ✓');
        }

        // Start Drip Scheduler (checks what drip messages to send NOW)
        if (CONFIG.DRIP_WORKER.ENABLED) {
            console.log('[App] Starting Drip Scheduler...');
            // Delay drip worker to allow other workers to initialize
            setTimeout(() => {
                dripWorker.start();
                console.log('[App] Drip Scheduler started ✓');
            }, 3000);
        }

        console.log('');
        console.log('╔════════════════════════════════════════════════════════════╗');
        console.log('║           All workers initialized successfully!            ║');
        console.log('║                                                            ║');
        console.log('║  Workers are now processing messages in the background.    ║');
        console.log('║  Press Ctrl+C to stop.                                     ║');
        console.log('╚════════════════════════════════════════════════════════════╝');
        console.log('');

        // Start queue monitoring (every 30 seconds)
        startQueueMonitor();

    } catch (error) {
        console.error('[App] Failed to start workers:', error);
        logger.error('[App] Failed to start workers:', error);
        process.exit(1);
    }
};

/**
 * Graceful shutdown
 */
const shutdown = async (signal) => {
    console.log('');
    console.log(`[App] Received ${signal}, shutting down gracefully...`);

    try {
        // Stop queue monitor
        stopQueueMonitor();

        // Stop Drip Scheduler first (it doesn't need RabbitMQ)
        if (CONFIG.DRIP_WORKER.ENABLED) {
            console.log('[App] Stopping Drip Scheduler...');
            dripWorker.stop();
        }

        // Stop Message Worker
        if (CONFIG.MESSAGE_WORKER.ENABLED) {
            console.log('[App] Stopping Message Worker...');
            await messageWorker.stop();
        }

        // Stop Inbound Worker
        if (CONFIG.MESSAGE_WORKER.ENABLED) {
            console.log('[App] Stopping Inbound Worker...');
            await inboundWorker.stop();
        }

        // Stop Status Worker
        if (CONFIG.MESSAGE_WORKER.ENABLED) {
            console.log('[App] Stopping Status Worker...');
            await statusWorker.stop();
        }

        // Close RabbitMQ connection
        if (CONFIG.RABBITMQ.ENABLED) {
            console.log('[App] Closing RabbitMQ connection...');
            await rabbitmq.close();
        }

        console.log('[App] Shutdown complete ✓');
        process.exit(0);

    } catch (error) {
        console.error('[App] Error during shutdown:', error);
        process.exit(1);
    }
};

// Handle shutdown signals
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// Handle uncaught errors
process.on('uncaughtException', (error) => {
    console.error('[App] Uncaught Exception:', error);
    logger.error('[App] Uncaught Exception:', error);
    shutdown('uncaughtException');
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('[App] Unhandled Rejection at:', promise, 'reason:', reason);
    logger.error('[App] Unhandled Rejection:', reason);
});

/**
 * Queue Monitor - Check queue depths and alert if too high
 */
let monitorInterval = null;
const QUEUE_ALERT_THRESHOLD = 100; // Alert if more than 100 messages waiting

const startQueueMonitor = () => {
    // Initial check after 10 seconds
    setTimeout(logQueueStats, 10000);

    // Then check every 30 seconds
    monitorInterval = setInterval(logQueueStats, 30000);
};

const logQueueStats = async () => {
    try {
        const stats = await rabbitmq.getAllQueueStats();

        if (!stats || stats.length === 0) return;

        // Check if any queue has high message count
        const highLoadQueues = stats.filter(q => q.messageCount > QUEUE_ALERT_THRESHOLD);

        if (highLoadQueues.length > 0) {
            console.log('');
            console.log('⚠️  [QueueMonitor] HIGH LOAD DETECTED:');
            highLoadQueues.forEach(q => {
                console.log(`   ${q.name}: ${q.messageCount} messages waiting (${q.consumerCount} consumers)`);
            });
            console.log('   Consider scaling workers: pm2 scale workers 4');
            console.log('');
        }

        // Log stats every 5 minutes (10 intervals of 30 seconds)
        const now = new Date();
        if (now.getMinutes() % 5 === 0 && now.getSeconds() < 30) {
            console.log('');
            console.log('[QueueMonitor] Queue Statistics:');
            console.log('┌────────────────────────────┬──────────┬───────────┐');
            console.log('│ Queue                      │ Messages │ Consumers │');
            console.log('├────────────────────────────┼──────────┼───────────┤');
            stats.forEach(q => {
                const name = q.name.padEnd(26);
                const msgs = String(q.messageCount).padStart(8);
                const cons = String(q.consumerCount).padStart(9);
                console.log(`│ ${name} │ ${msgs} │ ${cons} │`);
            });
            console.log('└────────────────────────────┴──────────┴───────────┘');
            console.log('');
        }

    } catch (error) {
        // Silent fail - don't spam logs if monitoring fails
    }
};

const stopQueueMonitor = () => {
    if (monitorInterval) {
        clearInterval(monitorInterval);
        monitorInterval = null;
    }
};

// Start the workers
startWorkers();
