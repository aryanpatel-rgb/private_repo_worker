/**
 * Twilio SMS/MMS Service for sengine-workers
 * Handles all Twilio API interactions for sending messages
 *
 * @module services/sms/twilio.service
 */

const { logger } = require('../logger.service');
const CONFIG = require('../../config/config');

/**
 * Initialize Twilio client with credentials
 */
const initTwilioClient = (credentials) => {
    const accountSid = credentials?.accountSid || CONFIG.TWILIO.ACCOUNT_SID;
    const authToken = credentials?.authToken || CONFIG.TWILIO.AUTH_TOKEN;

    if (!accountSid || !authToken) {
        logger.error('[Twilio] Missing credentials');
        throw new Error('Twilio credentials are required');
    }

    console.log('[Twilio] Initializing client with SID:', accountSid.substring(0, 10) + '...');

    const twilio = require('twilio');
    return twilio(accountSid, authToken);
};

/**
 * Format phone number to E.164 format
 */
const formatPhoneNumber = (phone) => {
    if (!phone) return null;

    let cleaned = phone.replace(/\D/g, '');

    if (cleaned.length === 10) {
        cleaned = '1' + cleaned;
    }

    if (!cleaned.startsWith('+')) {
        cleaned = '+' + cleaned;
    }

    return cleaned;
};

/**
 * Send SMS message via Twilio
 */
const sendSMS = async ({ from, to, body, mediaUrl, statusCallback, credentials }) => {
    console.log('[Twilio] sendSMS called:', {
        from,
        to,
        bodyLength: body?.length,
        hasMedia: !!mediaUrl,
        hasCallback: !!statusCallback
    });

    try {
        const client = initTwilioClient(credentials);

        const formattedFrom = formatPhoneNumber(from);
        const formattedTo = formatPhoneNumber(to);

        if (!formattedFrom || !formattedTo) {
            logger.error('[Twilio] Invalid phone numbers:', { from, to });
            throw new Error('Invalid phone numbers provided');
        }

        const messageOptions = {
            from: formattedFrom,
            to: formattedTo,
            body: body || ''
        };

        if (mediaUrl) {
            messageOptions.mediaUrl = Array.isArray(mediaUrl) ? mediaUrl : [mediaUrl];
        }

        if (statusCallback) {
            messageOptions.statusCallback = statusCallback;
        }

        console.log('[Twilio] Sending message:', {
            from: messageOptions.from,
            to: messageOptions.to,
            bodyLength: messageOptions.body?.length
        });

        const message = await client.messages.create(messageOptions);

        console.log('[Twilio] Message sent successfully:', {
            sid: message.sid,
            status: message.status
        });

        return {
            success: true,
            sid: message.sid,
            status: message.status,
            numSegments: message.numSegments,
            numMedia: message.numMedia,
            dateCreated: message.dateCreated,
            errorCode: null,
            errorMessage: null
        };

    } catch (error) {
        logger.error('[Twilio] Error sending message:', {
            error: error.message,
            code: error.code
        });

        return {
            success: false,
            sid: null,
            status: 'failed',
            numSegments: 0,
            numMedia: 0,
            dateCreated: null,
            errorCode: error.code || 'UNKNOWN',
            errorMessage: error.message
        };
    }
};

module.exports = {
    sendSMS,
    formatPhoneNumber,
    initTwilioClient
};
