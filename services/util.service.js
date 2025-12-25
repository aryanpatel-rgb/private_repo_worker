/**
 * Utility functions for sengine-workers
 */

const { to } = require('await-to-js');
const pe = require('parse-error');
const loggerService = require('./logger.service');

module.exports.to = async (promise) => {
    let err, res;
    [err, res] = await to(promise);
    if (err) {
        const parsedErr = pe(err);
        if (typeof parsedErr === 'string') {
            return [new Error(parsedErr)];
        }
        if (parsedErr instanceof Error) {
            return [parsedErr];
        }
        let errorMessage = '[object Object]';
        if (parsedErr && typeof parsedErr === 'object') {
            errorMessage = parsedErr.message ||
                          parsedErr.error ||
                          parsedErr.toString() ||
                          JSON.stringify(parsedErr);
        } else if (parsedErr) {
            errorMessage = String(parsedErr);
        }
        return [new Error(errorMessage)];
    }

    return [null, res];
};

module.exports.TE = TE = function (err_message, log) {
    if (log === true) {
        loggerService.error(err_message);
    }
    throw new Error(err_message);
};
