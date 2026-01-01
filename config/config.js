/**
 * Configuration for sengine-workers
 * Centralized configuration loading from environment variables
 */

const path = require('path');
const fs = require('fs');

// Load environment variables
const envPath = path.resolve(__dirname, '../.env');
if (fs.existsSync(envPath)) {
    require('dotenv').config({ path: envPath });
} else {
    console.warn(`[Config] .env file not found at ${envPath}. Using process environment.`);
}

/**
 * Parse DATABASE_URL into individual connection params
 */
function parseDatabaseUrl(url) {
    if (!url) return null;

    try {
        const parsed = new URL(url);
        return {
            host: parsed.hostname,
            port: parsed.port || '5432',
            user: parsed.username,
            password: parsed.password,
            database: parsed.pathname.slice(1)
        };
    } catch (err) {
        console.warn('[Config] Failed to parse DATABASE_URL:', err.message);
        return null;
    }
}

const dbUrl = process.env.DATABASE_URL;
const parsedDbUrl = dbUrl ? parseDatabaseUrl(dbUrl) : null;

const CONFIG = {
    APP: {
        ENVIRONMENT: process.env.APP || 'development'
    },

    DB: {
        DATABASE_URL: dbUrl || null,
        HOST: parsedDbUrl?.host || process.env.DB_HOST || 'localhost',
        PORT: parsedDbUrl?.port || process.env.DB_PORT || '5432',
        NAME: parsedDbUrl?.database || process.env.DB_NAME || 'postgres',
        USER: parsedDbUrl?.user || process.env.DB_USER || 'postgres',
        PASSWORD: parsedDbUrl?.password || process.env.DB_PASSWORD || ''
    },

    RABBITMQ: {
        ENABLED: process.env.RABBITMQ_ENABLED === 'true',
        URL: process.env.RABBITMQ_URL || 'amqp://localhost:5672'
    },

    TWILIO: {
        ACCOUNT_SID: process.env.TWILIO_ACCOUNT_SID || '',
        AUTH_TOKEN: process.env.TWILIO_AUTH_TOKEN || '',
        STATUS_CALLBACK_URL: process.env.TWILIO_STATUS_CALLBACK_URL || ''
    },

    // ==========================================================================
    // HIGH-SCALE DRIP CONFIGURATION (RabbitMQ-based)
    // ==========================================================================
    // This is the ONLY drip mode - optimized for very high volume (100K+ msgs/day)
    // Architecture: PreQueueWorker → RabbitMQ → MessageConsumer(s) → Twilio
    // ==========================================================================
    HIGH_SCALE_DRIP: {
        // Always enabled - this is the only drip processing mode
        ENABLED: process.env.DRIP_HIGH_SCALE_ENABLED !== 'false',

        // PreQueue Worker: Moves scheduled_messages → RabbitMQ
        PRE_QUEUE_WORKER_INTERVAL: parseInt(process.env.PRE_QUEUE_WORKER_INTERVAL || '30000', 10),  // Check every 30s for faster response
        PRE_QUEUE_MINUTES: parseInt(process.env.DRIP_PRE_QUEUE_MINUTES || '15', 10),               // Look ahead 15 minutes
        PRE_QUEUE_BATCH: parseInt(process.env.DRIP_PRE_QUEUE_BATCH || '2000', 10),                 // Queue 2000 at a time

        // Message Consumer: RabbitMQ → Twilio
        CONSUMER_PREFETCH: parseInt(process.env.DRIP_CONSUMER_PREFETCH || '50', 10),               // Hold 50 messages per consumer
        RATE_LIMIT_MS: parseInt(process.env.DRIP_RATE_LIMIT_MS || '25', 10),                       // 25ms = 40 msg/sec per consumer

        // Retry and batch settings
        MAX_RETRY: parseInt(process.env.DRIP_MAX_RETRY || '3', 10),
        BULK_INSERT_SIZE: parseInt(process.env.DRIP_BULK_INSERT_SIZE || '1000', 10),               // Batch DB inserts

        // Scaling settings
        CONCURRENT_CONSUMERS: parseInt(process.env.DRIP_CONCURRENT_CONSUMERS || '5', 10),          // Number of consumer instances
        MAX_MESSAGES_PER_SECOND: parseInt(process.env.DRIP_MAX_MESSAGES_PER_SECOND || '100', 10)   // Rate limit cap
    },

    MESSAGE_WORKER: {
        ENABLED: process.env.MESSAGE_WORKER_ENABLED !== 'false',
        PREFETCH: parseInt(process.env.MESSAGE_PREFETCH || '10', 10)
    }
};

module.exports = CONFIG;
