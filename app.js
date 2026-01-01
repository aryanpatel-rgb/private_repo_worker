/**
 * sengine-workers - Main Entry Point (HIGH-SCALE MODE)
 *
 * This application runs background workers for:
 * - Message sending (SEND_MESSAGE queue)
 * - Inbound message processing (INBOUND_MESSAGE queue)
 * - Delivery status updates (STATUS_UPDATE queue)
 * - Webhook dispatching (WEBHOOK queue)
 * - High-Scale Drip processing (scheduled_messages → RabbitMQ → Twilio)
 *
 * Architecture:
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │                    SENGINE-WORKERS (HIGH-SCALE)                     │
 * │                                                                     │
 * │  MESSAGE WORKERS:                                                   │
 * │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌───────────┐  │
 * │  │  Outbound   │  │   Inbound   │  │  Delivery   │  │  Webhook  │  │
 * │  │   Worker    │  │   Worker    │  │   Report    │  │   Worker  │  │
 * │  │  (Twilio)   │  │  (Process)  │  │  (Status)   │  │ (Dispatch)│  │
 * │  └─────────────┘  └─────────────┘  └─────────────┘  └───────────┘  │
 * │                                                                     │
 * │  HIGH-SCALE DRIP WORKERS:                                           │
 * │  ┌─────────────────────────────────────────────────────────────┐   │
 * │  │                     RabbitMQ Pipeline                        │   │
 * │  │                                                              │   │
 * │  │  ┌──────────────┐      ┌──────────────┐      ┌───────────┐  │   │
 * │  │  │  PreQueue    │ ───► │   RabbitMQ   │ ───► │  Message  │  │   │
 * │  │  │   Worker     │      │    Queue     │      │  Consumer │  │   │
 * │  │  │              │      │              │      │ (Scalable)│  │   │
 * │  │  │ DB → Queue   │      │ drip.messages│      │ → Twilio  │  │   │
 * │  │  └──────────────┘      └──────────────┘      └───────────┘  │   │
 * │  │                                                              │   │
 * │  │  Throughput: 50K-100K+ messages/day                          │   │
 * │  │  Scale with: pm2 scale workers N                             │   │
 * │  └─────────────────────────────────────────────────────────────┘   │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * Run with: node app.js
 * Or via PM2: pm2 start ecosystem.config.js
 * Scale workers: pm2 scale workers 4 (for 4x throughput)
 */

const path = require('path');

// Load environment variables
require('dotenv').config({ path: path.join(__dirname, '.env') });

const CONFIG = require('./config/config');
const { logger } = require('./services/logger.service');
const rabbitmq = require('./config/rabbitmq');

// Import workers
const outboundMessageWorker = require('./workers/outboundMessageWorker');
const inboundMessageWorker = require('./workers/inboundMessageWorker');
const deliveryReportWorker = require('./workers/deliveryReportWorker');
const webhookWorker = require('./workers/webhookWorker');

// High-Scale Drip Workers (RabbitMQ-based) - This is the ONLY drip processing mode
const preQueueWorker = require('./workers/preQueueWorker');
const messageConsumer = require('./workers/messageConsumer');

console.log('');
console.log('╔════════════════════════════════════════════════════════════╗');
console.log('║                    SENGINE WORKERS                         ║');
console.log('║         Background Processing Service (HIGH-SCALE)         ║');
console.log('╠════════════════════════════════════════════════════════════╣');
console.log('║                                                            ║');
console.log('║  Message Workers:                                          ║');
console.log('║  ├── Outbound Worker   : Send SMS via Twilio              ║');
console.log('║  ├── Inbound Worker    : Process incoming messages        ║');
console.log('║  ├── Delivery Report   : Update delivery status           ║');
console.log('║  └── Webhook Worker    : Dispatch user webhooks           ║');
console.log('║                                                            ║');
console.log('║  High-Scale Drip Workers (RabbitMQ-based):                 ║');
console.log('║  ├── PreQueue Worker   : scheduled_messages → RabbitMQ    ║');
console.log('║  └── Message Consumer  : RabbitMQ → Twilio (scalable)     ║');
console.log('║                                                            ║');
console.log('║  Scale consumers: pm2 scale workers N                      ║');
console.log('║                                                            ║');
console.log('╚════════════════════════════════════════════════════════════╝');
console.log('');
console.log('Configuration:');
console.log('  Environment    :', CONFIG.APP.ENVIRONMENT);
console.log('  Database       :', CONFIG.DB.DATABASE_URL ? 'Connected via URL' : `${CONFIG.DB.HOST}:${CONFIG.DB.PORT}`);
console.log('  RabbitMQ       :', CONFIG.RABBITMQ.ENABLED ? 'Enabled' : 'Disabled');
console.log('');
console.log('Workers Status:');
console.log('  Outbound Worker   :', CONFIG.MESSAGE_WORKER.ENABLED ? '✓ Enabled' : '✗ Disabled');
console.log('  Inbound Worker    :', CONFIG.MESSAGE_WORKER.ENABLED ? '✓ Enabled' : '✗ Disabled');
console.log('  Delivery Report   :', CONFIG.MESSAGE_WORKER.ENABLED ? '✓ Enabled' : '✗ Disabled');
console.log('  Webhook Worker    :', CONFIG.MESSAGE_WORKER.ENABLED ? '✓ Enabled' : '✗ Disabled');
console.log('');
console.log('High-Scale Drip:');
console.log('  PreQueue Worker   :', CONFIG.HIGH_SCALE_DRIP.ENABLED ? '✓ Enabled' : '✗ Disabled');
console.log('  Message Consumer  :', CONFIG.HIGH_SCALE_DRIP.ENABLED ? '✓ Enabled' : '✗ Disabled');
console.log('  PreQueue Interval :', CONFIG.HIGH_SCALE_DRIP.PRE_QUEUE_WORKER_INTERVAL + 'ms');
console.log('  PreQueue Batch    :', CONFIG.HIGH_SCALE_DRIP.PRE_QUEUE_BATCH);
console.log('  Consumer Prefetch :', CONFIG.HIGH_SCALE_DRIP.CONSUMER_PREFETCH);
console.log('  Rate Limit        :', CONFIG.HIGH_SCALE_DRIP.RATE_LIMIT_MS + 'ms');
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

        // Start Outbound Message Worker (sends SMS via Twilio)
        if (CONFIG.MESSAGE_WORKER.ENABLED) {
            console.log('[App] Starting Outbound Message Worker...');
            await outboundMessageWorker.start();
            console.log('[App] Outbound Message Worker started ✓');
        }

        // Start Inbound Message Worker (processes incoming messages)
        if (CONFIG.MESSAGE_WORKER.ENABLED) {
            console.log('[App] Starting Inbound Message Worker...');
            await inboundMessageWorker.start();
            console.log('[App] Inbound Message Worker started ✓');
        }

        // Start Delivery Report Worker (updates delivery status)
        if (CONFIG.MESSAGE_WORKER.ENABLED) {
            console.log('[App] Starting Delivery Report Worker...');
            await deliveryReportWorker.start();
            console.log('[App] Delivery Report Worker started ✓');
        }

        // Start Webhook Worker (dispatches user webhooks)
        if (CONFIG.MESSAGE_WORKER.ENABLED) {
            console.log('[App] Starting Webhook Worker...');
            await webhookWorker.start();
            console.log('[App] Webhook Worker started ✓');
        }

        // =======================================================================
        // HIGH-SCALE DRIP WORKERS (RabbitMQ-based)
        // This is the ONLY drip processing mode - optimized for 100K+ msgs/day
        // Architecture: PreQueueWorker → RabbitMQ → MessageConsumer(s) → Twilio
        // =======================================================================
        if (CONFIG.HIGH_SCALE_DRIP.ENABLED) {
            console.log('[App] Starting High-Scale Drip Workers...');

            // Start PreQueue Worker (pushes scheduled_messages to RabbitMQ)
            // Runs on interval, checks for messages ready to send
            setTimeout(async () => {
                await preQueueWorker.start();
                console.log('[App] PreQueue Worker started ✓');
                console.log(`     └─ Interval: ${CONFIG.HIGH_SCALE_DRIP.PRE_QUEUE_WORKER_INTERVAL}ms`);
                console.log(`     └─ Look-ahead: ${CONFIG.HIGH_SCALE_DRIP.PRE_QUEUE_MINUTES} minutes`);
                console.log(`     └─ Batch size: ${CONFIG.HIGH_SCALE_DRIP.PRE_QUEUE_BATCH}`);
            }, 3000);

            // Start Message Consumer (consumes from RabbitMQ, sends via Twilio)
            // This is the scalable component - run multiple instances for higher throughput
            setTimeout(async () => {
                await messageConsumer.start();
                console.log('[App] Message Consumer started ✓');
                console.log(`     └─ Prefetch: ${CONFIG.HIGH_SCALE_DRIP.CONSUMER_PREFETCH}`);
                console.log(`     └─ Rate limit: ${CONFIG.HIGH_SCALE_DRIP.RATE_LIMIT_MS}ms`);
            }, 4000);
        } else {
            console.warn('[App] ⚠️  High-Scale Drip is DISABLED - no drip messages will be sent!');
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

        // Stop High-Scale Drip Workers first (they have active consumers)
        if (CONFIG.HIGH_SCALE_DRIP.ENABLED) {
            console.log('[App] Stopping High-Scale Drip Workers...');
            await preQueueWorker.stop();
            await messageConsumer.stop();
        }

        // Stop Message Workers
        if (CONFIG.MESSAGE_WORKER.ENABLED) {
            console.log('[App] Stopping Outbound Message Worker...');
            await outboundMessageWorker.stop();

            console.log('[App] Stopping Inbound Message Worker...');
            await inboundMessageWorker.stop();

            console.log('[App] Stopping Delivery Report Worker...');
            await deliveryReportWorker.stop();

            console.log('[App] Stopping Webhook Worker...');
            await webhookWorker.stop();
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
