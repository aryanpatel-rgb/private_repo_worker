/**
 * Drip Scheduler Service for sengine-workers
 * Handles execution of drip campaigns (processing pending drips)
 *
 * Note: Enrollment functions are in main sengine (triggered by contact creation)
 * This worker only handles processing/sending of scheduled drips.
 *
 * @module services/drip/dripScheduler.service
 */

const { to, TE } = require('../util.service');
const { dbReader, dbWriter } = require('../../config/database');
const { logger } = require('../logger.service');
const messageService = require('../message/message.service');
const CONFIG = require('../../config/config');

// Drip contact status constants
const DRIP_STATUS = {
    PENDING: 0,
    SENT: 1,
    DELIVERED: 2,
    FAILED: 3,
    SKIPPED: 4,
    CANCELLED: 5
};

// Campaign contact drip status
const CAMPAIGN_DRIP_STATUS = {
    ACTIVE: 0,
    PAUSED: 1,
    COMPLETED: 2,
    STOPPED: 3
};

// Scaling configuration from environment
const SCALING_CONFIG = {
    BULK_BATCH_SIZE: CONFIG.DRIP_WORKER.BULK_BATCH_SIZE,
    CONCURRENT_DRIP_LIMIT: CONFIG.DRIP_WORKER.CONCURRENT_LIMIT,
    SMS_RATE_LIMIT_DELAY: CONFIG.DRIP_WORKER.RATE_LIMIT_DELAY,
    MAX_RETRY_COUNT: CONFIG.DRIP_WORKER.MAX_RETRIES,
    DRIP_WORKER_BATCH_SIZE: CONFIG.DRIP_WORKER.BATCH_SIZE
};

/**
 * Get all pending drips that are ready to be sent
 */
const getPendingDrips = async (limit = 100) => {
    console.log('[DripScheduler:Worker] Fetching pending drips...');

    try {
        const now = new Date();

        const [err, pendingDrips] = await to(
            dbReader('drip_contact')
                .select(
                    'drip_contact.*',
                    'drips.message_type',
                    'drips.message_suggestion',
                    'drips.title as drip_title',
                    'drips.media_path',
                    'drips.template_id',
                    'drips.ordering',
                    'contacts.phone',
                    'contacts.name as contact_name',
                    'contacts.email as contact_email',
                    'contacts.opted_out',
                    'contacts.is_block',
                    'campaigns.title as campaign_title',
                    'campaigns.status as campaign_status',
                    'campaign_contact.identifier as from_number_identifier'
                )
                .join('drips', dbReader.raw('drip_contact.drip_id::text'), 'drips.id')
                .join('contacts', 'drip_contact.contact_id', 'contacts.id')
                .join('campaigns', dbReader.raw('drip_contact.campaign_id::text'), 'campaigns.id')
                .leftJoin('campaign_contact', function() {
                    this.on(dbReader.raw('drip_contact.campaign_id::text'), '=', 'campaign_contact.campaign_id')
                        .andOn('drip_contact.contact_id', '=', 'campaign_contact.contact_id');
                })
                .where('drip_contact.status', DRIP_STATUS.PENDING)
                .where('drip_contact.scheduled_at', '<=', now)
                .where('campaigns.status', 1)
                .whereNull('contacts.deleted_at')
                .whereNull('drips.deleted_at')
                .whereNull('campaigns.deleted_at')
                .orderBy('drip_contact.scheduled_at', 'asc')
                .limit(limit)
        );

        if (err) {
            logger.error('[DripScheduler:Worker] Error fetching pending drips:', err);
            return [];
        }

        console.log('[DripScheduler:Worker] Found pending drips:', pendingDrips?.length || 0);
        return pendingDrips || [];

    } catch (error) {
        logger.error('[DripScheduler:Worker] getPendingDrips error:', error);
        return [];
    }
};

/**
 * Personalize message with contact data
 */
const personalizeMessage = (message, data) => {
    if (!message) return '';

    let personalized = message;

    personalized = personalized.replace(/\[first\]/gi, data.first || data.name || '');
    personalized = personalized.replace(/\[name\]/gi, data.name || '');
    personalized = personalized.replace(/\[phone\]/gi, data.phone || '');
    personalized = personalized.replace(/\[email\]/gi, data.email || '');
    personalized = personalized.replace(/\[campaign\]/gi, data.campaign || '');

    personalized = personalized.replace(/\{\{first\}\}/gi, data.first || data.name || '');
    personalized = personalized.replace(/\{\{name\}\}/gi, data.name || '');
    personalized = personalized.replace(/\{\{phone\}\}/gi, data.phone || '');
    personalized = personalized.replace(/\{\{email\}\}/gi, data.email || '');
    personalized = personalized.replace(/\{\{campaign\}\}/gi, data.campaign || '');

    return personalized.trim();
};

/**
 * Update drip_contact status after execution
 */
const updateDripContactStatus = async (dripContactId, status, errorMessage = null, messageId = null, bRef = null) => {
    const updateData = {
        status,
        updated_at: new Date()
    };

    if (status === DRIP_STATUS.SENT || status === DRIP_STATUS.DELIVERED) {
        updateData.sent_at = new Date();
    }

    if (errorMessage) {
        updateData.error_message = errorMessage;
    }

    if (messageId) {
        updateData.message_id = messageId;
    }

    if (bRef) {
        updateData.b_ref = bRef;
    }

    await to(
        dbWriter('drip_contact')
            .where({ id: dripContactId })
            .update(updateData)
    );
};

/**
 * Update campaign_contact progress after sending a drip
 */
const updateCampaignContactProgress = async (dripContact) => {
    try {
        const [nextErr, nextDrip] = await to(
            dbReader('drips')
                .where({ campaign_id: dripContact.campaign_id })
                .whereNull('deleted_at')
                .where('ordering', '>', dripContact.ordering || 0)
                .orderBy('ordering', 'asc')
                .first()
        );

        const updateData = {
            last_drip_sent_at: new Date(),
            updated_at: new Date()
        };

        if (nextDrip) {
            updateData.next_drip_id = nextDrip.id;
        } else {
            updateData.next_drip_id = null;
            updateData.drip_status = CAMPAIGN_DRIP_STATUS.COMPLETED;
        }

        await to(
            dbWriter('campaign_contact')
                .where({ campaign_id: dripContact.campaign_id, contact_id: dripContact.contact_id })
                .update(updateData)
        );

    } catch (error) {
        logger.warn('[DripScheduler:Worker] Error updating campaign contact progress:', error);
    }
};

/**
 * Execute a single drip (send the message)
 */
const executeDrip = async (dripContact, prefetchedUserNumbers = null) => {
    console.log('[DripScheduler:Worker] Executing drip:', {
        id: dripContact.id,
        dripId: dripContact.drip_id,
        contactId: dripContact.contact_id,
        contactName: dripContact.contact_name
    });

    try {
        // Check if contact is opted out or blocked
        if (dripContact.opted_out || dripContact.is_block) {
            console.log('[DripScheduler:Worker] Contact opted out or blocked, skipping');
            await updateDripContactStatus(dripContact.id, DRIP_STATUS.SKIPPED, 'Contact opted out or blocked');
            return { success: false, skipped: true, reason: 'Contact opted out or blocked' };
        }

        // Get the FROM number for sending
        let userNumber = null;
        const fromNumberIdentifier = dripContact.from_number_identifier;

        // Use pre-fetched numbers if available
        if (prefetchedUserNumbers && prefetchedUserNumbers.length > 0) {
            if (fromNumberIdentifier) {
                const normalizedIdentifier = fromNumberIdentifier.replace(/[\s\-\(\)\+]/g, '');
                userNumber = prefetchedUserNumbers.find(num => {
                    const normalizedPhone = num.phone.replace(/[\s\-\(\)\+]/g, '');
                    return num.phone === fromNumberIdentifier || normalizedPhone === normalizedIdentifier;
                });
            }
            if (!userNumber) {
                userNumber = prefetchedUserNumbers[0];
            }
            if (userNumber) {
                console.log('[DripScheduler:Worker] Using pre-fetched FROM number:', userNumber.phone);
            }
        }

        // If no pre-fetched number, query the database
        if (!userNumber) {
            console.log('[DripScheduler:Worker] Querying FROM number...');

            if (fromNumberIdentifier) {
                const normalizedIdentifier = fromNumberIdentifier.replace(/[\s\-\(\)\+]/g, '');
                const [specificNumErr, specificNumber] = await to(
                    dbWriter('user_numbers')
                        .where('user_id', String(dripContact.user_id))
                        .where('status', 1)
                        .whereNull('deleted_at')
                        .where(function() {
                            this.where('phone', fromNumberIdentifier)
                                .orWhereRaw("REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(phone, ' ', ''), '-', ''), '(', ''), ')', ''), '+', '') = ?", [normalizedIdentifier]);
                        })
                        .first()
                        .timeout(10000, { cancel: true })
                );

                if (!specificNumErr && specificNumber) {
                    userNumber = specificNumber;
                }
            }

            if (!userNumber) {
                const [numberErr, defaultNumber] = await to(
                    dbWriter('user_numbers')
                        .where('user_id', String(dripContact.user_id))
                        .where('status', 1)
                        .whereNull('deleted_at')
                        .first()
                        .timeout(10000, { cancel: true })
                );

                if (numberErr || !defaultNumber) {
                    console.log('[DripScheduler:Worker] No active user number found');
                    await updateDripContactStatus(dripContact.id, DRIP_STATUS.FAILED, 'No active sender number');
                    return { success: false, error: 'No active sender number' };
                }

                userNumber = defaultNumber;
            }
        }

        if (!userNumber) {
            await updateDripContactStatus(dripContact.id, DRIP_STATUS.FAILED, 'No active sender number');
            return { success: false, error: 'No active sender number' };
        }

        // Personalize message
        let message = dripContact.message_suggestion || '';
        message = personalizeMessage(message, {
            name: dripContact.contact_name,
            first: dripContact.contact_name?.split(' ')[0],
            phone: dripContact.phone,
            email: dripContact.contact_email,
            campaign: dripContact.campaign_title
        });

        console.log('[DripScheduler:Worker] Sending drip message:', {
            to: dripContact.phone,
            from: userNumber.phone,
            messageLength: message.length
        });

        // Send the message
        const sendResult = await messageService.sendMessage({
            userId: dripContact.user_id,
            workspaceId: dripContact.workspace_id,
            contactId: dripContact.contact_id,
            sid: userNumber.id,
            message: message,
            mediaUrl: dripContact.media_path || null,
            isDrip: true,
            dripId: dripContact.drip_id,
            dripContactId: dripContact.id
        });

        if (sendResult.success) {
            console.log('[DripScheduler:Worker] Drip sent successfully:', {
                messageId: sendResult.message?.id,
                bRef: sendResult.message?.b_ref
            });

            await updateDripContactStatus(
                dripContact.id,
                DRIP_STATUS.SENT,
                null,
                sendResult.message?.id,
                sendResult.message?.b_ref
            );

            await updateCampaignContactProgress(dripContact);

            return { success: true, messageId: sendResult.message?.id };
        } else {
            console.log('[DripScheduler:Worker] Drip send failed:', sendResult.twilioResponse?.errorMessage);
            await updateDripContactStatus(
                dripContact.id,
                DRIP_STATUS.FAILED,
                sendResult.twilioResponse?.errorMessage || 'Send failed'
            );
            return { success: false, error: sendResult.twilioResponse?.errorMessage };
        }

    } catch (error) {
        logger.error('[DripScheduler:Worker] executeDrip error:', error);
        await updateDripContactStatus(dripContact.id, DRIP_STATUS.FAILED, error.message);
        return { success: false, error: error.message };
    }
};

/**
 * Process all pending drips (main scheduler function)
 * This is called by the drip worker on a timer
 */
const processPendingDrips = async (batchSize = SCALING_CONFIG.DRIP_WORKER_BATCH_SIZE) => {
    const startTime = Date.now();
    console.log('[DripScheduler:Worker] Starting drip processing batch...');

    const results = {
        processed: 0,
        sent: 0,
        failed: 0,
        skipped: 0,
        errors: [],
        durationMs: 0
    };

    try {
        const pendingDrips = await getPendingDrips(batchSize);

        if (pendingDrips.length === 0) {
            console.log('[DripScheduler:Worker] No pending drips to process');
            return results;
        }

        console.log('[DripScheduler:Worker] Processing drips:', {
            total: pendingDrips.length,
            concurrencyLimit: SCALING_CONFIG.CONCURRENT_DRIP_LIMIT
        });

        // Get unique user IDs
        const userIds = [...new Set(pendingDrips.map(d => d.user_id))];
        console.log('[DripScheduler:Worker] Pre-fetching user numbers for', userIds.length, 'users...');

        // Pre-fetch all user numbers
        const [numbersErr, allUserNumbers] = await to(
            dbWriter('user_numbers')
                .whereIn('user_id', userIds)
                .where('status', 1)
                .whereNull('deleted_at')
                .timeout(30000, { cancel: true })
        );

        if (numbersErr) {
            logger.error('[DripScheduler:Worker] Failed to pre-fetch user numbers:', numbersErr);
            results.errors.push({ error: 'Failed to fetch user numbers' });
            return results;
        }

        // Create user numbers map
        const userNumbersMap = new Map();
        for (const num of (allUserNumbers || [])) {
            if (!userNumbersMap.has(num.user_id)) {
                userNumbersMap.set(num.user_id, []);
            }
            userNumbersMap.get(num.user_id).push(num);
        }

        console.log('[DripScheduler:Worker] Pre-fetched numbers for', userNumbersMap.size, 'users');

        // Process in batches
        const concurrencyLimit = SCALING_CONFIG.CONCURRENT_DRIP_LIMIT;

        for (let i = 0; i < pendingDrips.length; i += concurrencyLimit) {
            const batch = pendingDrips.slice(i, i + concurrencyLimit);

            console.log('[DripScheduler:Worker] Processing batch:', {
                batchStart: i,
                batchSize: batch.length,
                totalDrips: pendingDrips.length
            });

            // Process batch concurrently
            const batchPromises = batch.map(async (dripContact) => {
                try {
                    // Rate limiting delay
                    await new Promise(resolve => setTimeout(resolve, SCALING_CONFIG.SMS_RATE_LIMIT_DELAY));

                    const userNumbers = userNumbersMap.get(dripContact.user_id) || [];
                    const result = await executeDrip(dripContact, userNumbers);

                    if (result.success) {
                        results.sent++;
                    } else if (result.skipped) {
                        results.skipped++;
                    } else {
                        results.failed++;
                        results.errors.push({
                            dripContactId: dripContact.id,
                            error: result.error
                        });
                    }
                } catch (error) {
                    console.error('[DripScheduler:Worker] Error processing drip:', dripContact.id, error.message);
                    results.failed++;
                    results.errors.push({
                        dripContactId: dripContact.id,
                        error: error.message
                    });
                }
                results.processed++;
            });

            await Promise.all(batchPromises);
            console.log('[DripScheduler:Worker] Batch complete, processed so far:', results.processed);
        }

        results.durationMs = Date.now() - startTime;

        console.log('[DripScheduler:Worker] Batch complete:', {
            ...results,
            dripsPerSecond: Math.round((results.processed / results.durationMs) * 1000)
        });

        return results;

    } catch (error) {
        logger.error('[DripScheduler:Worker] processPendingDrips error:', error);
        results.errors.push({ error: error.message });
        results.durationMs = Date.now() - startTime;
        return results;
    }
};

/**
 * Get drip statistics for a campaign
 */
const getCampaignDripStats = async (campaignId) => {
    try {
        const [err, stats] = await to(
            dbReader('drip_contact')
                .select('status')
                .count('id as count')
                .where({ campaign_id: campaignId })
                .groupBy('status')
        );

        if (err) {
            logger.error('[DripScheduler:Worker] Error getting drip stats:', err);
            return null;
        }

        const result = {
            pending: 0,
            sent: 0,
            delivered: 0,
            failed: 0,
            skipped: 0,
            cancelled: 0,
            total: 0
        };

        for (const stat of stats || []) {
            const count = parseInt(stat.count, 10);
            result.total += count;

            switch (stat.status) {
                case DRIP_STATUS.PENDING: result.pending = count; break;
                case DRIP_STATUS.SENT: result.sent = count; break;
                case DRIP_STATUS.DELIVERED: result.delivered = count; break;
                case DRIP_STATUS.FAILED: result.failed = count; break;
                case DRIP_STATUS.SKIPPED: result.skipped = count; break;
                case DRIP_STATUS.CANCELLED: result.cancelled = count; break;
            }
        }

        return result;

    } catch (error) {
        logger.error('[DripScheduler:Worker] getCampaignDripStats error:', error);
        return null;
    }
};

module.exports = {
    DRIP_STATUS,
    CAMPAIGN_DRIP_STATUS,
    SCALING_CONFIG,
    getPendingDrips,
    executeDrip,
    processPendingDrips,
    personalizeMessage,
    getCampaignDripStats
};
