/**
 * Opt-Out Service for sengine-workers
 * Simplified version for checking opt-out status during drip execution
 *
 * @module services/message/optout.service
 */

const { to } = require('../util.service');
const { dbReader } = require('../../config/database');
const { logger } = require('../logger.service');

/**
 * Normalize phone number for comparison
 */
const normalizePhone = (phone) => {
    if (!phone) return '';
    return phone.replace(/\D/g, '');
};

/**
 * Check if a phone number is opted out for a user
 */
const checkOptOut = async (phone, userId) => {
    const normalizedPhone = normalizePhone(phone);

    const [err, optout] = await to(
        dbReader('contacts_optout')
            .where({ user_id: userId })
            .where(function() {
                this.where('phone', phone)
                    .orWhere('phone', normalizedPhone)
                    .orWhere('phone', '+' + normalizedPhone)
                    .orWhere('phone', '+1' + normalizedPhone);
            })
            .first()
    );

    if (err) {
        logger.error('[OptOut] Error checking opt-out status:', err);
        return false;
    }

    return !!optout;
};

/**
 * Check if a contact is opted out
 */
const checkContactOptOut = async (contactId) => {
    const [err, contact] = await to(
        dbReader('contacts')
            .select('opted_out')
            .where({ id: contactId })
            .first()
    );

    if (err) {
        logger.error('[OptOut] Error checking contact opt-out:', err);
        return false;
    }

    return contact?.opted_out === 1;
};

module.exports = {
    checkOptOut,
    checkContactOptOut,
    normalizePhone
};
