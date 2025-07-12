# Definition Worker

A NATS-based worker service that processes word definition requests using MCP (Model Context Protocol).

## Overview

This service:
- Consumes word definition requests from a NATS queue
- Uses MCP to generate definitions via various LLM providers
- Stores the generated definitions in PostgreSQL

## Setup

1. Install dependencies:
```bash
npm install
```

2. Copy `.env.example` to `.env` and configure:
```bash
cp .env.example .env
```

3. Run the service:
```bash
npm run dev
```

## Environment Variables

- `NATS_URL` - NATS server connection URL
- `DATABASE_URL` - PostgreSQL connection string
- `MCP_SERVER_PATH` - Path to MCP dictionary server
- `LLM_PROVIDER` - Default LLM provider (openai, anthropic, google, openrouter)

## Message Format

The service expects messages in the following format:
```json
{
  "word": "example",
  "language": "en",
  "provider": "openai" // optional, defaults to env var
}
```