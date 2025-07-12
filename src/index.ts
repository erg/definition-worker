import '@dotenvx/dotenvx';
import { connect, JSONCodec, type NatsConnection, type Subscription } from 'nats';
import { Pool } from 'pg';
import { MCPClient } from './mcp-client.js';
import { logger } from './logger.js';

interface DefinitionRequest {
  word: string;
  language: string;
  provider?: string;
  userId?: string;
}

interface DefinitionResult {
  word: string;
  language: string;
  definition: any;
  provider: string;
  timestamp: Date;
}

class DefinitionWorker {
  private nc: NatsConnection | null = null;
  private pool: Pool;
  private mcpClient: MCPClient;
  private subscription: Subscription | null = null;
  private codec = JSONCodec<DefinitionRequest>();

  constructor() {
    this.pool = new Pool({
      connectionString: process.env.DATABASE_URL,
    });
    
    this.mcpClient = new MCPClient(
      process.env.MCP_SERVER_PATH || '../mcp-dictionary'
    );
  }

  async start() {
    try {
      // Connect to NATS
      this.nc = await connect({
        servers: process.env.NATS_URL || 'nats://localhost:4222',
      });
      logger.info('Connected to NATS');

      // Initialize MCP client
      await this.mcpClient.connect();
      logger.info('Connected to MCP server');

      // Subscribe to the definition queue
      this.subscription = this.nc.subscribe('definitions.requests');
      logger.info('Subscribed to definitions.requests');

      // Process messages
      await this.processMessages();
    } catch (error) {
      logger.error('Failed to start worker:', error);
      throw error;
    }
  }

  private async processMessages() {
    if (!this.subscription) return;

    for await (const msg of this.subscription) {
      try {
        const request = this.codec.decode(msg.data);
        logger.info('Processing definition request:', request);

        // Generate definition using MCP
        const definition = await this.mcpClient.getDefinition(
          request.word,
          request.language,
          request.provider || process.env.LLM_PROVIDER || 'openai'
        );

        // Store in database
        await this.storeDefinition({
          word: request.word,
          language: request.language,
          definition: definition,
          provider: request.provider || process.env.LLM_PROVIDER || 'openai',
          timestamp: new Date(),
        });

        // Acknowledge the message
        msg.ack();
        logger.info(`Definition stored for: ${request.word}`);

        // Optionally publish result
        if (this.nc) {
          const resultCodec = JSONCodec<DefinitionResult>();
          await this.nc.publish(
            'definitions.results',
            resultCodec.encode({
              word: request.word,
              language: request.language,
              definition: definition,
              provider: request.provider || process.env.LLM_PROVIDER || 'openai',
              timestamp: new Date(),
            })
          );
        }
      } catch (error) {
        logger.error('Error processing message:', error);
        // Negative acknowledgment - message will be redelivered
        msg.nak();
      }
    }
  }

  private async storeDefinition(result: DefinitionResult) {
    const query = `
      INSERT INTO dictionary (
        word, language, definition, provider, created_at_utc
      ) VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (word, language) 
      DO UPDATE SET 
        definition = $3,
        provider = $4,
        updated_at_utc = $5
    `;

    await this.pool.query(query, [
      result.word,
      result.language,
      JSON.stringify(result.definition),
      result.provider,
      result.timestamp,
    ]);
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
}

// Start the worker
const worker = new DefinitionWorker();

worker.start().catch((error) => {
  logger.error('Worker failed to start:', error);
  process.exit(1);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  logger.info('SIGTERM received, shutting down...');
  await worker.stop();
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('SIGINT received, shutting down...');
  await worker.stop();
  process.exit(0);
});