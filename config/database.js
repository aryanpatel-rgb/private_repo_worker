/**
 * Database Configuration for sengine-workers
 * Connects to the same database as sengine using Knex
 */

'use strict';

const path = require('path');
const fs = require('fs');

// Load environment variables FIRST
const envPath = path.resolve(__dirname, '../.env');
if (fs.existsSync(envPath)) {
    require('dotenv').config({ path: envPath });
} else {
    console.warn(`[Database] .env file not found at ${envPath}. Falling back to process environment.`);
}

const knex = require('knex');
const { logger } = require('../services/logger.service');
const CONFIG = require('./config');

const db = {};

// Connection pool configuration optimized for workers
const dbWritePool = {
    min: 2,
    max: 20,
    acquireTimeoutMillis: 60000,
    createTimeoutMillis: 30000,
    idleTimeoutMillis: 30000,
    reapIntervalMillis: 10000,
    createRetryIntervalMillis: 200,
    propagateCreateError: false
};

const dbReadPool = {
    min: 2,
    max: 20,
    acquireTimeoutMillis: 60000,
    createTimeoutMillis: 30000,
    idleTimeoutMillis: 30000,
    reapIntervalMillis: 12000,
    createRetryIntervalMillis: 200,
    propagateCreateError: false
};

const getConnectionConfig = () => {
    if (CONFIG.DB.DATABASE_URL) {
        return CONFIG.DB.DATABASE_URL;
    }
    return {
        host: CONFIG.DB.HOST,
        port: parseInt(CONFIG.DB.PORT),
        user: CONFIG.DB.USER,
        password: String(CONFIG.DB.PASSWORD || '').trim(),
        database: CONFIG.DB.NAME
    };
};

const connectionConfig = getConnectionConfig();

const dbConfig = {
    writer: {
        client: 'pg',
        connection: connectionConfig,
        pool: dbWritePool,
        acquireConnectionTimeout: 60000
    },
    reader: {
        client: 'pg',
        connection: connectionConfig,
        pool: dbReadPool,
        acquireConnectionTimeout: 60000
    }
};

const dbWriter = knex(dbConfig.writer);
const dbReader = knex(dbConfig.reader);

// Test connection on startup
setImmediate(async () => {
    try {
        await dbReader.raw('SELECT 1');
        logger.info('[Workers] Database reader connection established successfully');
        console.log('[Workers] Database reader connection established successfully');
    } catch (err) {
        logger.error('[Workers] Database reader connection failed:', err.message);
        console.error('[Workers] Database reader connection failed:', err.message);
    }

    try {
        await dbWriter.raw('SELECT 1');
        logger.info('[Workers] Database writer connection established successfully');
        console.log('[Workers] Database writer connection established successfully');
    } catch (err) {
        logger.error('[Workers] Database writer connection failed:', err.message);
        console.error('[Workers] Database writer connection failed:', err.message);
    }
});

db.dbWriter = dbWriter;
db.dbReader = dbReader;

module.exports = db;
