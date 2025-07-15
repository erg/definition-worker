import { logger } from './logger.js';
import { getClickHouseClient } from '@language-app/logger/clickhouse';

interface MCPResponse {
  result?: any;
  error?: {
    code: number;
    message: string;
    data?: any;
  };
}

export class MCPClientHTTP {
  private baseUrl: string;
  private clickhouse: any;

  constructor(serverUrl: string = 'http://localhost:3002') {
    this.baseUrl = serverUrl;
    try {
      this.clickhouse = getClickHouseClient();
    } catch (error) {
      logger.warn('ClickHouse client not available in MCP client');
    }
  }

  async connect(): Promise<void> {
    try {
      // Test connection with timeout
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 5000);
      
      const response = await fetch(`${this.baseUrl}/health`, {
        signal: controller.signal
      });
      clearTimeout(timeout);
      
      if (!response.ok) {
        throw new Error(`MCP server health check failed: ${response.status}`);
      }
      logger.info('Connected to MCP server via HTTP');
    } catch (error: any) {
      if (error.name === 'AbortError') {
        logger.error('MCP server connection timeout');
        throw new Error('MCP server connection timeout');
      }
      logger.error('Failed to connect to MCP server:', error);
      throw error;
    }
  }

  async getDefinition(word: string, language: string, provider: string, context?: string, isTooltip?: boolean): Promise<any> {
    try {
      // Map provider names to valid LLM providers
      const llmProvider = provider === 'mcp-claude' ? 'lmstudio' : provider;
      logger.info(`Requesting definition for ${word} using provider ${llmProvider}${isTooltip ? ' (tooltip mode)' : ''}`);
      
      // Add timeout to prevent hanging
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 60000); // 60 second timeout
      
      const response = await fetch(`${this.baseUrl}/generate-definition`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          word,
          language,
          context: isTooltip ? 'tooltip' : context,  // Pass 'tooltip' as context to trigger concise mode
          includeEtymology: !isTooltip,  // Skip extras for tooltips
          includeCollocations: !isTooltip,
          includeExpressions: !isTooltip,
          includeCulturalNotes: !isTooltip,
          llmProvider: llmProvider
        }),
        signal: controller.signal
      });
      clearTimeout(timeout);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${await response.text()}`);
      }

      const data: MCPResponse = await response.json();
      
      if (data.error) {
        throw new Error(data.error.message);
      }

      logger.info(`Received definition response for ${word}:`, JSON.stringify(data).substring(0, 200) + '...');
      return data.result || data;
    } catch (error: any) {
      if (error.name === 'AbortError') {
        logger.error(`Definition request timeout for ${word} after 60 seconds`);
        
        // Log timeout error to ClickHouse
        if (this.clickhouse) {
          await this.clickhouse.logEvent({
            service: 'mcp-client',
            event_type: 'definition_timeout',
            level: 'error',
            word,
            language,
            provider,
            error_message: `Definition request timeout for ${word}`,
            metadata: {
              timeout_ms: 60000,
              base_url: this.baseUrl
            }
          });
        }
        
        // Throw error instead of returning a fallback definition
        throw new Error(`Definition request timeout for ${word}`);
      }
      
      logger.error(`Failed to get definition for ${word}:`, {
        error: error.message,
        stack: error.stack,
        code: error.code,
        url: `${this.baseUrl}/generate-definition`
      });
      
      // Log other errors to ClickHouse
      if (this.clickhouse) {
        await this.clickhouse.logEvent({
          service: 'mcp-client',
          event_type: 'definition_error',
          level: 'error',
          word,
          language,
          provider,
          error_message: error.message,
          metadata: {
            base_url: this.baseUrl,
            error_type: error.name
          }
        });
      }
      
      throw error;
    }
  }

  async disconnect() {
    // No-op for HTTP client
    logger.info('Disconnected from MCP server');
  }
}