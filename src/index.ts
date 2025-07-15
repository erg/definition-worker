import { connect, NatsConnection, JetStream, ConsumerConfig, Consumer, JSONCodec, JsMsg } from 'nats';
import { Pool } from 'pg';
import { MCPClient } from './mcp-client.js';
import { MCPClientHTTP } from './mcp-client-http.js';
import { logger } from './logger.js';
import { getClickHouseClient } from '@language-app/logger/clickhouse';

interface DefinitionRequest {
  word: string;
  language: string;
  provider?: string;
  userId: number;
  timestamp: string;
  context?: string; // Optional sentence context for better definitions
}

interface DefinitionResult {
  word: string;
  language: string;
  definition: any;
  provider: string;
  timestamp: Date;
}

class DefinitionWorker {
  private nc!: NatsConnection;
  private js!: JetStream;
  private pool: Pool;
  private mcpClient: MCPClient | MCPClientHTTP;
  private subscription: any;
  private codec = JSONCodec<DefinitionRequest>();

  constructor() {
    this.pool = new Pool({
      connectionString: process.env.DATABASE_URL
    });
    
    // Choose MCP client based on configuration
    if (process.env.MCP_SERVER_URL) {
      logger.info(`MCP_SERVER_URL: ${process.env.MCP_SERVER_URL}`);
      logger.info(`MCP_SERVER_PATH: ${process.env.MCP_SERVER_PATH}`);
      logger.info('Using HTTP client for MCP');
      this.mcpClient = new MCPClientHTTP(process.env.MCP_SERVER_URL);
    } else if (process.env.MCP_SERVER_PATH) {
      logger.info('Using process-based client for MCP');
      this.mcpClient = new MCPClient(process.env.MCP_SERVER_PATH);
    } else {
      throw new Error('Either MCP_SERVER_URL or MCP_SERVER_PATH must be set');
    }
  }

  async start() {
    try {
      // Initialize ClickHouse
      let clickhouse: any;
      try {
        clickhouse = getClickHouseClient();
        logger.info('ClickHouse client initialized successfully');
      } catch (error) {
        logger.warn('ClickHouse client initialization failed:', error);
      }
      
      // Connect to NATS
      this.nc = await connect({ 
        servers: process.env.NATS_URL || 'nats://localhost:4222',
        reconnect: true,
        maxReconnectAttempts: -1,  // Infinite reconnect attempts
        reconnectTimeWait: 2000,    // Wait 2 seconds between reconnect attempts
        pingInterval: 10000,        // Ping every 10 seconds
        maxPingOut: 3              // Allow 3 missed pings before disconnect
      });
      logger.info('Connected to NATS');
      
      // Log connection events
      (async () => {
        for await (const status of this.nc.status()) {
          logger.info(`NATS connection status: ${status.type}`, status.data);
          
          // Log to ClickHouse
          if (clickhouse) {
            await clickhouse.logEvent({
              service: 'definition-worker',
              event_type: 'nats_connection_status',
              level: status.type === 'disconnect' ? 'warn' : 'info',
              metadata: {
                status_type: status.type,
                status_data: status.data
              }
            });
          }
        }
      })();
      
      // Handle connection closed
      this.nc.closed().then((err) => {
        if (err) {
          logger.error('NATS connection closed with error:', err);
        } else {
          logger.info('NATS connection closed');
        }
      });
      
      // Connect to MCP server
      await this.mcpClient.connect();
      logger.info('Connected to MCP server');

      // Get JetStream context
      this.js = this.nc.jetstream();
      
      // Create or update stream
      const jsm = await this.js.jetstreamManager();
      try {
        const stream = await jsm.streams.info('DEFINITIONS');
        logger.info('Using existing DEFINITIONS stream');
      } catch (err) {
        // Stream doesn't exist, create it
        await jsm.streams.add({
          name: 'DEFINITIONS',
          subjects: ['definitions.requests', 'definitions.results'],
          retention: 'workqueue',
          storage: 'file',
          max_msgs: 10000,
          max_msg_size: 1024 * 1024, // 1MB
          max_age: 7 * 24 * 60 * 60 * 1e9, // 7 days in nanoseconds
          discard: 'old',
          duplicate_window: 60 * 1e9, // 1 minute duplicate window
        });
        logger.info('Created DEFINITIONS stream');
      }
      
      // Create durable consumer with proper configuration
      const consumerName = 'definition-worker';
      const consumerConfig: ConsumerConfig = {
        durable_name: consumerName,
        deliver_policy: 'all', // Start from beginning to reprocess all messages
        ack_policy: 'explicit',
        ack_wait: 90 * 1e9, // 90 seconds in nanoseconds (match LM Studio timeout)
        max_deliver: 3,      // Reduce retries to avoid loops
        filter_subject: 'definitions.requests',
        max_ack_pending: 1,  // CRITICAL: Only allow 1 unacknowledged message at a time
        // Remove deliver_subject to make it a pull consumer
      };
      
      // Delete existing consumer and recreate to ensure clean state
      try {
        await jsm.consumers.delete('DEFINITIONS', consumerName);
        logger.info(`Deleting existing consumer: ${consumerName}`);
      } catch (err) {
        // Consumer doesn't exist, that's fine
      }
      
      // Create the consumer
      await jsm.consumers.add('DEFINITIONS', consumerConfig);
      logger.info(`Created ${consumerName} consumer to reprocess all messages`);
      
      // Get consumer handle
      logger.info(`Getting consumer ${consumerName} from DEFINITIONS stream...`);
      const consumer = await this.js.consumers.get('DEFINITIONS', consumerName);
      logger.info('Consumer retrieved successfully');
      
      // Get consumer info to check for pending messages
      const info = await consumer.info();
      logger.info('Consumer info:', {
        name: info.name,
        num_pending: info.num_pending,
        num_ack_pending: info.num_ack_pending,
        delivered: info.delivered,
        ack_floor: info.ack_floor,
        config: info.config
      });
      
      // Get stream info to understand message sequences
      const streamInfo = await jsm.streams.info('DEFINITIONS');
      logger.info('Stream info:', {
        messages: streamInfo.state.messages,
        bytes: streamInfo.state.bytes,
        first_seq: streamInfo.state.first_seq,
        last_seq: streamInfo.state.last_seq,
        consumer_count: streamInfo.state.consumer_count
      });
      
      // Check if there are pending messages
      if (info.num_pending > 0) {
        logger.info(`Found ${info.num_pending} pending messages to process`);
      }
      
      // TODO: Process multiple messages at once for better throughput
      // TODO: Consider grouping by language to optimize LLM context switching
      logger.info('Starting consumer.consume()...');
      logger.info('Consumer type:', consumer.constructor.name);
      
      // For pull-based consumers, we need to fetch messages explicitly
      // IMPORTANT: Set max_messages to 1 to prevent overwhelming LM Studio
      this.subscription = await consumer.consume({
        max_messages: 1,  // Process one at a time to prevent overwhelming the system
        expires: 30000,   // 30 second timeout for fetching messages
        idle_heartbeat: 5000,  // Send heartbeats every 5 seconds
        batch: 1,         // Only pull one message at a time
        max_waiting: 1    // Only allow one waiting pull request
      });
      
      logger.info('Consumer.consume() completed, subscription created');
      logger.info('Subscription type:', typeof this.subscription);
      logger.info('Subscription properties:', Object.keys(this.subscription || {}));
      logger.info('Subscribed to definitions.requests with JetStream');
      
      // Try to manually pull a message to test
      logger.info('Testing manual pull...');
      if ('pull' in this.subscription) {
        logger.info('Subscription has pull method, attempting pull...');
        try {
          (this.subscription as any).pull({ expires: 1000 });
          logger.info('Pull initiated');
        } catch (pullError) {
          logger.error('Pull error:', pullError);
        }
      }

      // Start periodic consumer health check
      setInterval(async () => {
        try {
          const consumerInfo = await consumer.info();
          logger.info('Consumer health check:', {
            num_pending: consumerInfo.num_pending,
            num_ack_pending: consumerInfo.num_ack_pending,
            delivered: consumerInfo.delivered
          });
          
          // Log to ClickHouse
          if (clickhouse) {
            await clickhouse.logEvent({
              service: 'definition-worker',
              event_type: 'consumer_health_check',
              level: 'info',
              metadata: {
                num_pending: consumerInfo.num_pending,
                num_ack_pending: consumerInfo.num_ack_pending,
                delivered_consumer_seq: consumerInfo.delivered.consumer_seq,
                delivered_stream_seq: consumerInfo.delivered.stream_seq
              }
            });
          }
        } catch (error) {
          logger.error('Health check failed:', error);
        }
      }, 10000); // Every 10 seconds
      
      // Process messages
      await this.processMessages();
    } catch (error) {
      logger.error('Failed to start worker:', error);
      throw error;
    }
  }

  private async processMessage(msg: any) {
    const { clickhouse } = (await import('@language-app/logger/clickhouse'));
    
    // Log to ClickHouse
    if (clickhouse) {
      await clickhouse.logEvent({
        service: 'definition-worker',
        event_type: 'message_received',
        level: 'info',
        message_subject: msg.subject
      });
    }
    
    try {
      const request = JSON.parse(msg.data.toString());
      logger.info('Processing definition request:', request);
      
      // Generate definition (with retry logic)
      let definition;
      let retryCount = 0;
      const maxRetries = 3;
      const baseDelay = 2000; // 2 seconds
      
      while (retryCount <= maxRetries) {
        try {
          definition = await this.mcpClient.getDefinition(
            request.word,
            request.language,
            request.provider || process.env.LLM_PROVIDER || 'openai',
            request.context
          );
          break; // Success, exit retry loop
        } catch (mcpError: any) {
          const isTimeout = mcpError.message?.includes('timeout');
          const isRetryable = isTimeout || mcpError.message?.includes('ECONNREFUSED');
          
          if (retryCount < maxRetries && isRetryable) {
            // Calculate exponential backoff delay
            const delay = baseDelay * Math.pow(2, retryCount);
            logger.warn(`Attempt ${retryCount + 1} failed for ${request.word}, retrying in ${delay}ms...`, {
              error: mcpError.message,
              isTimeout,
              provider: request.provider || process.env.LLM_PROVIDER
            });
            
            // Log retry attempt to ClickHouse
            if (clickhouse) {
              await clickhouse.logEvent({
                service: 'definition-worker',
                event_type: 'definition_retry',
                level: 'warn',
                word: request.word,
                language: request.language,
                metadata: {
                  retry_count: retryCount + 1,
                  delay_ms: delay,
                  error_type: isTimeout ? 'timeout' : 'connection',
                  provider: request.provider || process.env.LLM_PROVIDER
                }
              });
            }
            
            // Wait before retrying
            await new Promise(resolve => setTimeout(resolve, delay));
            retryCount++;
          } else {
            // Max retries exceeded or non-retryable error
            logger.error(`Failed to get definition for ${request.word} after ${retryCount + 1} attempts:`, {
              error: mcpError.message || 'Unknown error',
              stack: mcpError.stack,
              code: mcpError.code,
              details: mcpError
            });
            
            // Log final failure to ClickHouse
            if (clickhouse) {
              await clickhouse.logEvent({
                service: 'definition-worker',
                event_type: 'definition_failed',
                level: 'error',
                word: request.word,
                language: request.language,
                error_message: mcpError.message,
                metadata: {
                  total_attempts: retryCount + 1,
                  provider: request.provider || process.env.LLM_PROVIDER,
                  is_timeout: isTimeout
                }
              });
            }
            
            // Requeue the message for later retry by nacking it
            // NATS will retry based on consumer's ack_wait configuration
            msg.nak();
            logger.info(`Message nacked for ${request.word} - will be retried later`);
            
            // Add a small delay before processing next message to prevent rapid loops
            await new Promise(resolve => setTimeout(resolve, 1000));
            return;
          }
        }
      }

      // Store in database (only successful definitions)
      await this.storeDefinition({
        word: request.word,
        language: request.language,
        definition: definition,
        provider: request.provider || process.env.LLM_PROVIDER || 'lmstudio',
        timestamp: new Date(),
      });

      // Acknowledge the message
      logger.info(`About to acknowledge message for: ${request.word}`);
      msg.ack();
      logger.info(`Message acknowledged successfully for: ${request.word}`);
      logger.info(`Definition stored for: ${request.word}`);

      // Optionally publish result
      if (this.nc) {
        const resultCodec = JSONCodec<DefinitionResult>();
        logger.info(`Publishing result to definitions.results for: ${request.word}`);
        this.nc.publish('definitions.results', resultCodec.encode({
          word: request.word,
          language: request.language,
          definition: definition,
          provider: request.provider || process.env.LLM_PROVIDER || 'lmstudio',
          timestamp: new Date()
        }));
      }
    } catch (error: any) {
      logger.error('Failed to process message:', error);
      
      // Log critical error to ClickHouse
      if (clickhouse) {
        await clickhouse.logEvent({
          service: 'definition-worker',
          event_type: 'message_processing_error',
          level: 'error',
          error_message: error.message,
          metadata: {
            stack: error.stack,
            message_subject: msg.subject
          }
        });
      }
      
      // Always acknowledge to prevent getting stuck
      msg.ack();
      logger.warn(`Acknowledged failed message`);
      
      // Add a small delay before processing next message
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  private async processMessages() {
    if (!this.subscription) {
      logger.error('No subscription available!');
      return;
    }

    logger.info('Starting message processing loop...');
    logger.info('Subscription status:', {
      closed: this.subscription.closed,
      isPullMode: (this.subscription as any).isPullMode,
      hasPending: (this.subscription as any).pending > 0,
      pending: (this.subscription as any).pending
    });
    
    let messageCount = 0;
    
    // TODO: Implement batch processing logic:
    // 1. Collect messages up to a batch size or timeout
    // 2. Group by language for efficient processing
    // 3. Send multiple requests to MCP in parallel
    // 4. Store results in a single transaction
    
    logger.info('Entering for await loop...');
    logger.info('Subscription iterator check:', {
      isIterable: Symbol.asyncIterator in this.subscription,
      hasNext: 'next' in this.subscription
    });
    
    // For pull-based consumers, continuously pull messages
    logger.info('Starting pull-based message consumption...');
    
    // Use async iterator to process messages
    try {
      logger.info('Starting message processing with strict serial processing...');
      
      for await (const msg of this.subscription) {
        messageCount++;
        const startTime = Date.now();
        logger.info(`=== MESSAGE RECEIVED #${messageCount} === (Total in queue: ${(msg as any).info?.pending || 'unknown'})`);
        
        // CRITICAL: Process the message and wait for it to complete
        // This ensures we only process one message at a time
        await this.processMessage(msg);
        
        const processingTime = Date.now() - startTime;
        logger.info(`Completed processing message #${messageCount} in ${processingTime}ms`);
        
        // Add a small delay to ensure LM Studio isn't overwhelmed
        if (processingTime < 1000) {
          await new Promise(resolve => setTimeout(resolve, 100));
        }
      } // End of for await loop
    } catch (error: any) {
      logger.error('Error in message processing loop:', error);
      
      // Log critical error to ClickHouse
      const { clickhouse: ch } = await import('@language-app/logger/clickhouse');
      if (ch) {
        await ch.logEvent({
          service: 'definition-worker',
          event_type: 'message_loop_error',
          level: 'error',
          error_message: error.message,
          metadata: {
            stack: error.stack
          }
        });
      }
      
      throw error;
    }
    
    logger.info('Message processing loop ended');
    logger.info(`Total messages processed: ${messageCount}`);
  }

  private async storeDefinition(result: DefinitionResult) {
    // Don't store timeout or error definitions
    if (result.definition?.error === 'timeout' || 
        result.definition?.definition === 'Definition temporarily unavailable') {
      logger.warn(`Skipping storage of timeout definition for: ${result.word}`);
      return;
    }
    
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      
      // Get or create dictionary entry
      const dictResult = await client.query(
        `SELECT id FROM dictionary WHERE language_code = $1 AND word = $2`,
        [result.language, result.word]
      );
      
      let dictionaryId: number;
      if (dictResult.rows.length === 0) {
        const insertResult = await client.query(
          `INSERT INTO dictionary (language_code, word, part_of_speech, created_at_utc) 
           VALUES ($1, $2, $3, NOW()) RETURNING id`,
          [result.language, result.word, 'unknown']
        );
        dictionaryId = insertResult.rows[0].id;
      } else {
        dictionaryId = dictResult.rows[0].id;
      }
      
      // Insert definition
      await client.query(
        `INSERT INTO definitions (dictionary_id, definition, example_sentence, usage_notes, formality_level, source, ai_model, definition_json, generation_version, generated_at_utc)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
         ON CONFLICT (dictionary_id, ai_model, generation_version) 
         DO UPDATE SET 
          definition = EXCLUDED.definition,
          example_sentence = EXCLUDED.example_sentence,
          usage_notes = EXCLUDED.usage_notes,
          formality_level = EXCLUDED.formality_level,
          definition_json = EXCLUDED.definition_json,
          source = EXCLUDED.source,
          generated_at_utc = NOW()`,
        [
          dictionaryId,
          result.definition.definition || result.definition.content || JSON.stringify(result.definition),
          result.definition.example_sentence || null,
          result.definition.usage_notes || null,
          result.definition.formality_level || 'neutral',
          'ai_generated',
          result.provider,
          result.definition,
          1  // generation_version
        ]
      );
      
      await client.query('COMMIT');
      logger.info(`Stored definition for ${result.word} with dictionary_id ${dictionaryId}`);
      
      // Fun console output for successful definitions
      const emoji = result.language === 'ru' ? '🇷🇺' : result.language === 'en' ? '🇬🇧' : '🌍';
      console.log(`${emoji} Definition generated: "${result.word}" - ${result.definition.definition?.substring(0, 60)}...`);
      
      // Log to ClickHouse
      const { clickhouse: ch } = await import('@language-app/logger/clickhouse');
      if (ch) {
        await ch.logDefinitionMetric({
          word: result.word,
          language: result.language,
          provider: result.provider,
          dictionary_id: dictionaryId,
          processing_time_ms: 0, // We don't track this yet
          success: true
        });
      }
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async stop() {
    if (this.subscription) {
      await this.subscription.unsubscribe();
    }
    if (this.nc) {
      await this.nc.close();
    }
    await this.mcpClient.disconnect();
    await this.pool.end();
    logger.info('Worker stopped');
  }
  
  private async testLLMConnection(): Promise<void> {
    // Try to get a simple definition to test if LLM is working
    const testResult = await this.mcpClient.getDefinition('test', 'en', 'lmstudio');
    if (testResult.error === 'timeout' || testResult.error === 'service_error') {
      throw new Error('LLM service is not responding');
    }
  }
}

// Start the worker
logger.info('Starting definition worker...');
logger.info('Current environment:', {
  MCP_SERVER_URL: process.env.MCP_SERVER_URL,
  MCP_SERVER_PATH: process.env.MCP_SERVER_PATH,
  NODE_ENV: process.env.NODE_ENV
});

let worker: DefinitionWorker;

try {
  worker = new DefinitionWorker();
  worker.start().catch((error) => {
    logger.error('Worker failed to start:', error);
    process.exit(1);
  });
} catch (error: any) {
  logger.error('Failed to create worker:', {
    message: error.message,
    stack: error.stack,
    error: error
  });
  process.exit(1);
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received, shutting down...');
  if (worker) {
    await worker.stop();
  }
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('SIGINT received, shutting down...');
  if (worker) {
    await worker.stop();
  }
  process.exit(0);
});