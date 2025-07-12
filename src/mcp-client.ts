import { spawn, ChildProcess } from 'child_process';
import { logger } from './logger.js';

interface MCPRequest {
  jsonrpc: '2.0';
  method: string;
  params: any;
  id: number;
}

interface MCPResponse {
  jsonrpc: '2.0';
  result?: any;
  error?: {
    code: number;
    message: string;
    data?: any;
  };
  id: number;
}

export class MCPClient {
  private process: ChildProcess | null = null;
  private requestId = 0;
  private pendingRequests = new Map<number, {
    resolve: (result: any) => void;
    reject: (error: any) => void;
  }>();
  private buffer = '';

  constructor(private serverPath: string) {}

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.process = spawn('node', [this.serverPath], {
        stdio: ['pipe', 'pipe', 'pipe'],
        env: {
          ...process.env,
        },
      });

      this.process.stdout?.on('data', (data) => {
        this.buffer += data.toString();
        this.processBuffer();
      });

      this.process.stderr?.on('data', (data) => {
        logger.error('MCP Server error:', data.toString());
      });

      this.process.on('error', (error) => {
        logger.error('Failed to start MCP server:', error);
        reject(error);
      });

      this.process.on('exit', (code) => {
        logger.info(`MCP server exited with code ${code}`);
        this.pendingRequests.forEach(({ reject }) => {
          reject(new Error('MCP server exited unexpectedly'));
        });
        this.pendingRequests.clear();
      });

      // Wait for server to be ready
      setTimeout(() => {
        this.sendRequest('system.info', {})
          .then(() => {
            logger.info('MCP server is ready');
            resolve();
          })
          .catch(reject);
      }, 1000);
    });
  }

  private processBuffer() {
    const lines = this.buffer.split('\\n');
    this.buffer = lines.pop() || '';

    for (const line of lines) {
      if (line.trim()) {
        try {
          const response: MCPResponse = JSON.parse(line);
          const pending = this.pendingRequests.get(response.id);
          if (pending) {
            this.pendingRequests.delete(response.id);
            if (response.error) {
              pending.reject(new Error(response.error.message));
            } else {
              pending.resolve(response.result);
            }
          }
        } catch (error) {
          logger.error('Failed to parse MCP response:', error, line);
        }
      }
    }
  }

  private async sendRequest(method: string, params: any): Promise<any> {
    return new Promise((resolve, reject) => {
      const id = ++this.requestId;
      const request: MCPRequest = {
        jsonrpc: '2.0',
        method,
        params,
        id,
      };

      this.pendingRequests.set(id, { resolve, reject });

      if (!this.process?.stdin) {
        reject(new Error('MCP server not connected'));
        return;
      }

      this.process.stdin.write(JSON.stringify(request) + '\\n');

      // Timeout after 30 seconds
      setTimeout(() => {
        if (this.pendingRequests.has(id)) {
          this.pendingRequests.delete(id);
          reject(new Error('Request timeout'));
        }
      }, 30000);
    });
  }

  async getDefinition(word: string, language: string, provider: string): Promise<any> {
    try {
      const result = await this.sendRequest('tools/generate_dictionary', {
        word,
        target_language: language,
        provider,
      });
      return result;
    } catch (error) {
      logger.error(`Failed to get definition for ${word}:`, error);
      throw error;
    }
  }

  async disconnect() {
    if (this.process) {
      this.process.kill();
      this.process = null;
    }
  }
}