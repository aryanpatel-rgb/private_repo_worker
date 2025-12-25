/**
 * PM2 Ecosystem Configuration for sengine-workers
 *
 * This configuration allows running workers with PM2 for:
 * - Process management
 * - Auto-restart on crash
 * - Log management
 * - Scaling
 *
 * Usage:
 *   pm2 start ecosystem.config.js                    # Start all workers
 *   pm2 start ecosystem.config.js --only workers     # Start message/inbound/status workers
 *   pm2 start ecosystem.config.js --only drip        # Start drip scheduler only
 *   pm2 scale workers 4                              # Scale workers to 4 instances
 *   pm2 logs                                         # View logs
 *   pm2 monit                                        # Monitor
 *
 * Architecture:
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │                      PM2 PROCESS MANAGEMENT                         │
 * │                                                                     │
 * │  ┌─────────────────────────────────────────────────────────────┐   │
 * │  │  "workers" (CAN SCALE - multiple instances OK)              │   │
 * │  │                                                              │   │
 * │  │  Each instance runs:                                         │   │
 * │  │  - Message Worker  (sends SMS via Twilio)                   │   │
 * │  │  - Inbound Worker  (processes incoming messages)            │   │
 * │  │  - Status Worker   (updates delivery status)                │   │
 * │  │                                                              │   │
 * │  │  Scale command: pm2 scale workers 4                         │   │
 * │  └─────────────────────────────────────────────────────────────┘   │
 * │                                                                     │
 * │  ┌─────────────────────────────────────────────────────────────┐   │
 * │  │  "drip" (ONLY 1 INSTANCE - prevents duplicate scheduling)   │   │
 * │  │                                                              │   │
 * │  │  Runs every minute to check:                                 │   │
 * │  │  "What drip messages need to send NOW?"                      │   │
 * │  │                                                              │   │
 * │  │  DO NOT SCALE! Only 1 instance allowed!                     │   │
 * │  └─────────────────────────────────────────────────────────────┘   │
 * └─────────────────────────────────────────────────────────────────────┘
 */

module.exports = {
    apps: [
        // ============================================================
        // WORKERS: Message, Inbound, Status workers
        // CAN SCALE to multiple instances for high throughput
        // ============================================================
        {
            name: 'workers',
            script: './app.js',
            cwd: __dirname,

            // Scaling - start with 1, scale with: pm2 scale workers 4
            instances: 1,
            exec_mode: 'fork',

            // Environment
            env: {
                NODE_ENV: 'development',
                DRIP_WORKER_ENABLED: 'false',    // Drip runs separately
                MESSAGE_WORKER_ENABLED: 'true',
                RABBITMQ_ENABLED: 'true'
            },
            env_production: {
                NODE_ENV: 'production',
                DRIP_WORKER_ENABLED: 'false',
                MESSAGE_WORKER_ENABLED: 'true',
                RABBITMQ_ENABLED: 'true'
            },

            // Auto-restart
            autorestart: true,
            watch: false,
            max_memory_restart: '500M',
            restart_delay: 5000,

            // Logging
            log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
            error_file: './logs/workers-error.log',
            out_file: './logs/workers-out.log',
            merge_logs: true,
            time: true,

            // Graceful shutdown
            kill_timeout: 10000,
            max_restarts: 10,
            min_uptime: '10s'
        },

        // ============================================================
        // DRIP SCHEDULER: Checks what drip messages to send NOW
        // ONLY 1 INSTANCE! Do not scale - will cause duplicates!
        // ============================================================
        {
            name: 'drip',
            script: './workers/dripWorker.js',
            cwd: __dirname,

            // IMPORTANT: Only 1 instance!
            instances: 1,
            exec_mode: 'fork',

            // Environment
            env: {
                NODE_ENV: 'development',
                DRIP_BATCH_SIZE: '200',
                DRIP_INTERVAL_MS: '60000',      // Check every 1 minute
                DRIP_CONCURRENT_LIMIT: '25'
            },
            env_production: {
                NODE_ENV: 'production',
                DRIP_BATCH_SIZE: '500',
                DRIP_INTERVAL_MS: '60000',
                DRIP_CONCURRENT_LIMIT: '50'
            },

            // Auto-restart
            autorestart: true,
            watch: false,
            max_memory_restart: '300M',
            restart_delay: 10000,              // Longer delay to avoid duplicate runs

            // Logging
            log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
            error_file: './logs/drip-error.log',
            out_file: './logs/drip-out.log',
            merge_logs: true,
            time: true,

            // Graceful shutdown
            kill_timeout: 5000,
            max_restarts: 5,
            min_uptime: '30s'
        },

        // ============================================================
        // STANDALONE WORKERS (for separate scaling if needed)
        // Use these if you want to scale each worker type independently
        // ============================================================

        // Standalone Message Worker
        {
            name: 'message-worker',
            script: './workers/messageWorker.js',
            cwd: __dirname,
            instances: 1,
            exec_mode: 'fork',
            autorestart: false,    // Disabled by default - use 'workers' instead
            env: {
                NODE_ENV: 'development',
                MESSAGE_PREFETCH: '10'
            },
            env_production: {
                NODE_ENV: 'production',
                MESSAGE_PREFETCH: '20'
            },
            error_file: './logs/message-worker-error.log',
            out_file: './logs/message-worker-out.log',
            time: true
        },

        // Standalone Inbound Worker
        {
            name: 'inbound-worker',
            script: './workers/inboundWorker.js',
            cwd: __dirname,
            instances: 1,
            exec_mode: 'fork',
            autorestart: false,    // Disabled by default
            env: {
                NODE_ENV: 'development'
            },
            error_file: './logs/inbound-worker-error.log',
            out_file: './logs/inbound-worker-out.log',
            time: true
        },

        // Standalone Status Worker
        {
            name: 'status-worker',
            script: './workers/statusWorker.js',
            cwd: __dirname,
            instances: 1,
            exec_mode: 'fork',
            autorestart: false,    // Disabled by default
            env: {
                NODE_ENV: 'development'
            },
            error_file: './logs/status-worker-error.log',
            out_file: './logs/status-worker-out.log',
            time: true
        }
    ]
};
