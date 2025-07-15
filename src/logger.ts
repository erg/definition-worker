import { createLogger } from '@language-app/logger';
import path from 'path';

// Create winston logger instance
const winstonLogger = createLogger({
  service: 'definition-worker',
  logDir: path.join(process.cwd(), '../../logs'),
  level: process.env.LOG_LEVEL || 'info'
});

// Export logger with the same interface
export const logger = {
  info: (message: string, ...args: any[]) => {
    winstonLogger.info(message, { data: args });
  },
  error: (message: string, ...args: any[]) => {
    winstonLogger.error(message, { data: args });
  },
  warn: (message: string, ...args: any[]) => {
    winstonLogger.warn(message, { data: args });
  },
  debug: (message: string, ...args: any[]) => {
    winstonLogger.debug(message, { data: args });
  },
};