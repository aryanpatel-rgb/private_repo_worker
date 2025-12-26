/**
 * Credits Service for sengine-workers
 * Handles credit check and deduction for drip messages
 */

const { dbReader, dbWriter } = require('../config/database');
const { to } = require('./util.service');
const { logger } = require('./logger.service');

/**
 * Check if user has enough credits
 * @param {string} userId - User ID
 * @param {number} amount - Required credit amount
 * @returns {Promise<boolean>} True if user has enough credits
 */
const hasEnoughCredits = async (userId, amount) => {
    if (!userId) return false;

    const [err, credits] = await to(
        dbReader('user_credits').where({ user_id: userId }).first()
    );

    if (err || !credits) {
        console.log('[Credits] No credits record found for user:', userId);
        return false;
    }

    return credits.balance >= amount;
};

/**
 * Deduct credits from user account
 * @param {string} userId - User ID
 * @param {number} amount - Credit amount to deduct
 * @param {Object} options - Transaction options
 * @returns {Promise<Object>} Updated credits and transaction
 */
const deductCredits = async (userId, amount, options = {}) => {
    if (!userId) throw new Error('User ID is required');
    if (!amount || amount <= 0) throw new Error('Valid credit amount is required');

    const {
        description = 'Credits used',
        referenceType = null,
        referenceId = null,
    } = options;

    const trx = await dbWriter.transaction();

    try {
        // Get user credits
        const credits = await trx('user_credits').where({ user_id: userId }).first();

        if (!credits) {
            await trx.rollback();
            throw new Error('User credits not found');
        }

        // Check if user has enough credits
        if (credits.balance < amount) {
            await trx.rollback();
            throw new Error('Insufficient credits');
        }

        const newBalance = credits.balance - amount;
        const newTotalSpent = credits.total_spent + amount;

        // Update user credits
        await trx('user_credits')
            .where({ user_id: userId })
            .update({
                balance: newBalance,
                total_spent: newTotalSpent,
            });

        // Create transaction record
        const [transaction] = await trx('credit_transactions')
            .insert({
                user_id: userId,
                type: 'debit',
                amount: -amount,
                balance_after: newBalance,
                description: description,
                reference_type: referenceType,
                reference_id: referenceId,
            })
            .returning('*');

        await trx.commit();

        console.log(`[Credits] Deducted ${amount} credits from user ${userId}. New balance: ${newBalance}`);

        return {
            credits: {
                balance: newBalance,
                totalSpent: newTotalSpent,
            },
            transaction,
        };
    } catch (error) {
        await trx.rollback();
        logger.error('[Credits] Error deducting credits:', error);
        throw error;
    }
};

module.exports = {
    hasEnoughCredits,
    deductCredits,
};
