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

    DRIP_WORKER: {
        ENABLED: process.env.DRIP_WORKER_ENABLED !== 'false',
        BATCH_SIZE: parseInt(process.env.DRIP_BATCH_SIZE || '200', 10),
        INTERVAL_MS: parseInt(process.env.DRIP_INTERVAL_MS || '60000', 10),
        MAX_RETRIES: parseInt(process.env.DRIP_MAX_RETRIES || '3', 10),
        BULK_BATCH_SIZE: parseInt(process.env.DRIP_BULK_BATCH_SIZE || '500', 10),
        CONCURRENT_LIMIT: parseInt(process.env.DRIP_CONCURRENT_LIMIT || '25', 10),
        RATE_LIMIT_DELAY: parseInt(process.env.DRIP_RATE_LIMIT_DELAY || '50', 10)
    },

    // High-Scale Drip Configuration (RabbitMQ-based)
    HIGH_SCALE_DRIP: {
        ENABLED: process.env.DRIP_HIGH_SCALE_ENABLED === 'true',
        PRE_QUEUE_WORKER_INTERVAL: parseInt(process.env.PRE_QUEUE_WORKER_INTERVAL || '60000', 10),
        PRE_QUEUE_MINUTES: parseInt(process.env.DRIP_PRE_QUEUE_MINUTES || '10', 10),
        PRE_QUEUE_BATCH: parseInt(process.env.DRIP_PRE_QUEUE_BATCH || '1000', 10),
        CONSUMER_PREFETCH: parseInt(process.env.DRIP_CONSUMER_PREFETCH || '10', 10),
        RATE_LIMIT_MS: parseInt(process.env.DRIP_RATE_LIMIT_MS || '50', 10),
        MAX_RETRY: parseInt(process.env.DRIP_MAX_RETRY || '3', 10),
        BULK_INSERT_SIZE: parseInt(process.env.DRIP_BULK_INSERT_SIZE || '500', 10)
    },

    MESSAGE_WORKER: {
        ENABLED: process.env.MESSAGE_WORKER_ENABLED !== 'false',
        PREFETCH: parseInt(process.env.MESSAGE_PREFETCH || '10', 10)
    }
};

module.exports = CONFIG;
