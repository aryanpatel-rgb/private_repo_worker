/**
 * Centralized logging utility for sengine-workers
 */

const CONFIG = require('../config/config');

class Logger {
    static isLoggingEnabled() {
        return CONFIG.APP.ENVIRONMENT === 'development';
    }

    static log(...args) {
        if (this.isLoggingEnabled()) {
            console.log('[Workers]', ...args);
        }
    }

    static error(...args) {
        console.error('[Workers:ERROR]', ...args);
    }

    static warn(...args) {
        if (this.isLoggingEnabled()) {
            console.warn('[Workers:WARN]', ...args);
        }
    }

    static debug(...args) {
        if (this.isLoggingEnabled()) {
            console.debug('[Workers:DEBUG]', ...args);
        }
    }

    static info(...args) {
        if (this.isLoggingEnabled()) {
            console.info('[Workers:INFO]', ...args);
        }
    }
}

const logger = Logger;

module.exports = Logger;
module.exports.Logger = Logger;
module.exports.logger = logger;
