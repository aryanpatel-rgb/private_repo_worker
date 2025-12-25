  # Start all workers
  pm2 start ecosystem.config.js

  # Start specific workers only
  pm2 start ecosystem.config.js --only workers    # Message/Inbound/Status workers
  pm2 start ecosystem.config.js --only drip       # Drip scheduler only

  # Scale workers (for high load)
  pm2 scale workers 4

  # Monitor & logs
  pm2 logs
  pm2 monit



  How Drip Trigger Works

  Database Tables Used

  1. drips table (Drip definition)

  | Column             | Description                               |
  |--------------------|-------------------------------------------|
  | id                 | Unique drip ID                            |
  | campaign_id        | Which campaign this drip belongs to       |
  | duration           | Time value (e.g., 1, 5, 24)               |
  | duration_type      | Unit: minutes, hours, days, weeks, months |
  | drip_time          | Optional specific time (deprecated)       |
  | ordering           | Order in sequence (1st, 2nd, 3rd drip)    |
  | message_suggestion | The message content                       |
  | message_type       | sms, mms, email, postcard                 |

  2. drip_contact table (Scheduled messages)

  | Column       | Description                                         |
  |--------------|-----------------------------------------------------|
  | drip_id      | Which drip this is for                              |
  | contact_id   | Which contact                                       |
  | campaign_id  | Which campaign                                      |
  | scheduled_at | When to send (calculated from enrollment)           |
  | status       | 0=Pending, 1=Sent, 2=Delivered, 3=Failed, 4=Skipped |
  | sent_at      | When actually sent                                  |

  3. campaign_contact table (Enrollment tracking)

  | Column       | Description                                    |
  |--------------|------------------------------------------------|
  | enrolled_at  | When contact was enrolled (trigger start time) |
  | drip_status  | 0=Active, 1=Paused, 2=Completed, 3=Stopped     |
  | next_drip_id | Next drip to send                              |

  ---
  How the Trigger Flow Works

  1. ENROLLMENT (Contact added to campaign)
     └── enrolled_at = NOW()

  2. SCHEDULING (For each drip in sequence)
     └── scheduled_at = enrolled_at + cumulative_duration

     Example with 3 drips:
     ┌─────────────────────────────────────────────────────────┐
     │ Drip 1: 5 minutes  → scheduled_at = enrolled + 5 min   │
     │ Drip 2: 1 hour     → scheduled_at = enrolled + 1h 5min │
     │ Drip 3: 1 day      → scheduled_at = enrolled + 1d 1h 5m│
     └─────────────────────────────────────────────────────────┘

  3. WORKER (Runs every minute)
     └── SELECT * FROM drip_contact
         WHERE status = 0 (pending)
         AND scheduled_at <= NOW()
     └── Send message via Twilio
     └── Update status to 1 (sent)

  ---
  Simple Example

  Contact enrolled: Dec 25, 10:00 AM

  Drip 1: duration=1, duration_type=minutes
    → scheduled_at = 10:01 AM

  Drip 2: duration=5, duration_type=minutes
    → scheduled_at = 10:06 AM (cumulative: 1+5=6 min)

  Drip 3: duration=1, duration_type=hours
    → scheduled_at = 11:06 AM (cumulative: 6min + 1hr)

  ---
  Key Properties

  | Property                 | Table            | Purpose                                       |
  |--------------------------|------------------|-----------------------------------------------|
  | enrolled_at              | campaign_contact | Start time for calculating all drip schedules |
  | duration + duration_type | drips            | How long to wait before sending               |
  | scheduled_at             | drip_contact     | Exact time when message will be sent          |
  | status                   | drip_contact     | Whether message is pending/sent/failed        |

  Would you like me to explain any specific part in more detail?





  
  Problem with Long Queue Hold

  ❌ BAD: Queue holds 50K messages for 6 months

  Issues:
  - RabbitMQ restarts → Messages LOST
  - Memory usage: 50K × 6 months = HUGE
  - Not reliable for long-term storage
  - RabbitMQ designed for short-term queuing only

  ---
  CORRECT Solution: Hybrid Approach

  Database = Long-term storage (6 months) ✅
  RabbitMQ = Short-term delivery (5-10 minutes before) ✅

  ---
  Architecture: Database + Pre-Queue Worker

           ENROLLMENT (Day 0)
                  │
                  ▼
      ┌───────────────────────┐
      │ Create message entry  │
      │ in DATABASE           │
      │                       │
      │ status = 'scheduled'  │
      │ send_at = 6 months    │
      └───────────┬───────────┘
                  │
                  │ (Stored safely in DB for 6 months)
                  │
                  ▼
           6 MONTHS LATER...
                  │
                  ▼
      ┌───────────────────────┐
      │ PRE-QUEUE WORKER      │
      │ Runs every 5 minutes  │
      │                       │
      │ Finds messages where: │
      │ send_at <= NOW() + 10 │
      │ minutes               │
      └───────────┬───────────┘
                  │
                  │ (Only queue 10 min before send time)
                  │
                  ▼
      ┌───────────────────────┐
      │ Push to RabbitMQ      │
      │ (Short-term only)     │
      └───────────┬───────────┘
                  │
          ┌───────┴───────┐
          ▼               ▼
     ┌─────────┐     ┌─────────┐
     │Consumer1│     │Consumer2│
     │  SEND   │     │  SEND   │
     └─────────┘     └─────────┘

  ---
  How It Works

  | Step | When               | Where            | What                                   |
  |------|--------------------|------------------|----------------------------------------|
  | 1    | Enrollment         | Database         | Create message with send_at = 6 months |
  | 2    | 6 months (waiting) | Database         | Message stored safely                  |
  | 3    | 10 min before send | Pre-Queue Worker | Find due messages                      |
  | 4    | 10 min before send | RabbitMQ         | Queue messages                         |
  | 5    | Send time          | Consumers        | Send via Twilio                        |

  ---
  Timeline Example

  Day 0 (Jan 1):
    └── Contact enrolled, drip = 6 months
    └── Message created in DB: send_at = July 1, 10:00 AM
    └── status = 'scheduled'

  ... 6 months pass (message safe in DB) ...

  July 1, 9:50 AM:
    └── Pre-Queue Worker runs
    └── Finds: "Message due at 10:00 AM (in 10 min)"
    └── Pushes to RabbitMQ
    └── status = 'queued'

  July 1, 10:00 AM:
    └── Consumer picks up message
    └── Sends via Twilio
    └── status = 'sent'

  ---
  Database Table: scheduled_messages

  | Column          | Type      | Description                  |
  |-----------------|-----------|------------------------------|
  | id              | BIGSERIAL | Primary key                  |
  | user_id         | CHAR(36)  | User ID                      |
  | workspace_id    | BIGINT    | Workspace                    |
  | contact_id      | BIGINT    | Contact                      |
  | drip_id         | UUID      | Drip reference               |
  | drip_contact_id | BIGINT    | Drip contact reference       |
  | from_number     | VARCHAR   | Sender phone                 |
  | to_number       | VARCHAR   | Recipient phone              |
  | message         | TEXT      | Message content              |
  | media_url       | TEXT      | MMS media (if any)           |
  | send_at         | TIMESTAMP | When to send                 |
  | status          | VARCHAR   | scheduled/queued/sent/failed |
  | created_at      | TIMESTAMP | Created time                 |
  | queued_at       | TIMESTAMP | When pushed to queue         |
  | sent_at         | TIMESTAMP | When actually sent           |

  ---
  Pre-Queue Worker

  // Runs every 5 minutes
  const preQueueWorker = async () => {
      // Find messages due in next 10 minutes
      const dueMessages = await db('scheduled_messages')
          .where('status', 'scheduled')
          .where('send_at', '<=', NOW() + 10 minutes)
          .limit(10000);

      // Push to RabbitMQ
      for (const batch of chunks(dueMessages, 1000)) {
          await rabbitmq.publishBatch('drip-send', batch);

          // Update status to 'queued'
          await db('scheduled_messages')
              .whereIn('id', batch.map(m => m.id))
              .update({ status: 'queued', queued_at: new Date() });
      }
  };

  ---
  Summary

  | Storage  | Duration     | Purpose                     |
  |----------|--------------|-----------------------------|
  | Database | 6 months     | Long-term, reliable storage |
  | RabbitMQ | 5-10 minutes | Short-term, fast delivery   |

  ---
  Benefits

  ✅ Reliable - Database won't lose messages
  ✅ Scalable - Handle millions of scheduled messages
  ✅ Fast - Messages queued just before send time
  ✅ Accurate - Sent within seconds of scheduled time
  ✅ No memory issues - RabbitMQ holds only recent messages