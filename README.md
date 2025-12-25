# sengine-workers

Background worker service for sengine - handles drip campaigns and message queue processing.

## Overview

This service runs independently from the main sengine API and processes:

1. **Drip Worker** - Executes scheduled drip campaign messages
2. **Message Worker** - Consumes messages from RabbitMQ queue and sends via Twilio

## Architecture

```
┌──────────────────┐     ┌───────────────┐     ┌─────────────────────┐
│   sengine API    │────▶│   RabbitMQ    │◀────│   sengine-workers   │
│   (Port 3001)    │     │  (Port 5672)  │     │   (Background)      │
└──────────────────┘     └───────────────┘     └─────────────────────┘
         │                                              │
         │                                              │
         ▼                                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         PostgreSQL Database                          │
│                         (Port 5432)                                  │
└──────────────────────────────────────────────────────────────────────┘
```

## Setup

### 1. Install Dependencies

```bash
cd sengine-workers
npm install
```

### 2. Configure Environment

The `.env` file should match your sengine configuration:

```env
DATABASE_URL=postgresql://postgres:postgres123@45.55.94.153:5432/postgres
RABBITMQ_URL=amqp://inbox_user:inbox_password_2024@localhost:5672
TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxx
TWILIO_AUTH_TOKEN=xxxxxxxxxxxxxxxx
```

### 3. Disable Workers in Main sengine

In `sengine/.env`, add:

```env
DRIP_WORKER_ENABLED=false
```

### 4. Start Workers

**Development:**
```bash
npm start
```

**Production with PM2:**
```bash
pm2 start ecosystem.config.js
```

## Running Individual Workers

You can run workers separately for more granular scaling:

```bash
# Just the drip worker
npm run drip-worker

# Just the message worker
npm run message-worker
```

Or with PM2:

```bash
pm2 start ecosystem.config.js --only drip-worker
pm2 start ecosystem.config.js --only message-worker
```

## Configuration

### Drip Worker Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `DRIP_WORKER_ENABLED` | `true` | Enable/disable drip worker |
| `DRIP_BATCH_SIZE` | `200` | Drips to process per batch |
| `DRIP_INTERVAL_MS` | `60000` | Interval between batches (ms) |
| `DRIP_CONCURRENT_LIMIT` | `25` | Max concurrent drip sends |
| `DRIP_RATE_LIMIT_DELAY` | `50` | Delay between SMS sends (ms) |

### Message Worker Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `MESSAGE_WORKER_ENABLED` | `true` | Enable/disable message worker |
| `MESSAGE_PREFETCH` | `10` | Messages to prefetch from queue |

## Scaling

### Horizontal Scaling

For high throughput, you can run multiple worker instances:

```bash
# Multiple drip workers (different configurations)
DRIP_BATCH_SIZE=500 pm2 start ecosystem.config.js --only drip-worker -i 2

# Multiple message workers
pm2 start ecosystem.config.js --only message-worker -i 4
```

### Vertical Scaling

Adjust these settings for higher throughput:

```env
DRIP_BATCH_SIZE=500
DRIP_CONCURRENT_LIMIT=50
DRIP_INTERVAL_MS=30000
MESSAGE_PREFETCH=20
```

## Monitoring

### PM2 Dashboard

```bash
pm2 monit
pm2 logs sengine-workers
pm2 status
```

### Queue Status

Check RabbitMQ management UI at http://localhost:15672

## Logs

Logs are stored in the `logs/` directory:
- `output.log` - Standard output
- `error.log` - Errors
- `drip-output.log` - Drip worker logs
- `message-output.log` - Message worker logs

## Troubleshooting

### RabbitMQ Connection Failed

1. Ensure RabbitMQ is running: `rabbitmq-server`
2. Check credentials in `.env`
3. Verify port 5672 is open

### Database Connection Failed

1. Check DATABASE_URL in `.env`
2. Ensure PostgreSQL is accessible
3. Verify network connectivity

### No Drips Being Processed

1. Check `drip_contact` table has pending records
2. Verify `scheduled_at` is in the past
3. Confirm campaign is active (status = 1)

## Development

### Run in Development Mode

```bash
npm run dev  # Uses nodemon for auto-reload
```

### Test Single Batch

```bash
node -e "require('./workers/dripWorker').runOnce()"
```
