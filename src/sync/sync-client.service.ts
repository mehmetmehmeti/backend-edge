import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { RawData, WebSocket } from 'ws';
import { OUTBOX_EVENT_CREATED, SyncOutboxService } from './sync-outbox.service';
import { SyncService } from './sync.service';

@Injectable()
export class SyncClientService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SyncClientService.name);
  private socket?: WebSocket;
  private reconnectTimer?: NodeJS.Timeout;
  private flushTimer?: NodeJS.Timeout;
  private stopped = false;

  constructor(
    private readonly syncService: SyncService,
    private readonly syncOutbox: SyncOutboxService,
  ) {}

  onModuleInit() {
    this.connect();
    this.syncOutbox.on(OUTBOX_EVENT_CREATED, () => {
      void this.sendPendingEvents();
    });
    this.flushTimer = setInterval(() => {
      void this.sendPendingEvents();
    }, 2000);
  }

  onModuleDestroy() {
    this.stopped = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
    }
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
    }
    this.socket?.close();
  }

  private connect() {
    const baseUrl = process.env.CLOUD_SYNC_URL?.trim();
    const token = process.env.EDGE_SYNC_TOKEN?.trim();
    if (!baseUrl || !token) {
      this.logger.warn('CLOUD_SYNC_URL and EDGE_SYNC_TOKEN are required for edge sync');
      return;
    }

    const url = new URL(baseUrl);
    url.searchParams.set('token', token);
    this.socket = new WebSocket(url);

    this.socket.on('open', () => {
      this.logger.log(`Connected to cloud sync at ${baseUrl}`);
      void this.sendPendingEvents();
    });
    this.socket.on('message', (message) => {
      void this.handleMessage(message);
    });
    this.socket.on('close', () => this.scheduleReconnect());
    this.socket.on('error', (error) => {
      this.logger.warn(`Cloud sync error: ${error.message}`);
    });
  }

  private scheduleReconnect() {
    if (this.stopped || this.reconnectTimer) {
      return;
    }
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      this.connect();
    }, 2000);
  }

  private async handleMessage(raw: RawData) {
    let payload: unknown;
    try {
      payload = JSON.parse(this.rawToString(raw));
    } catch {
      this.sendJson({
        type: 'ack',
        event_id: 'unknown',
        status: 'failed',
        source: 'edge',
        occurred_at: new Date().toISOString(),
        message: 'Sync message must be valid JSON',
      });
      return;
    }

    if (this.isAck(payload)) {
      try {
        const ack = this.syncService.parseAck(payload);
        await this.syncOutbox.applyAck(ack.event_id, ack.status, ack.message);
      } catch (error) {
        this.logger.warn(error instanceof Error ? error.message : 'Invalid sync ack');
      }
      return;
    }

    const ack = await this.syncService.processEvent(payload);
    this.sendJson(ack);
  }

  private async sendPendingEvents() {
    if (this.socket?.readyState !== WebSocket.OPEN) {
      return;
    }
    const events = await this.syncOutbox.listPendingEvents();
    for (const event of events) {
      this.sendJson(event);
      await this.syncOutbox.markSent(event.event_id);
    }
  }

  private sendJson(payload: object) {
    if (this.socket?.readyState === WebSocket.OPEN) {
      this.socket.send(JSON.stringify(payload));
    }
  }

  private rawToString(raw: RawData) {
    if (Buffer.isBuffer(raw)) {
      return raw.toString('utf8');
    }
    if (Array.isArray(raw)) {
      return Buffer.concat(raw).toString('utf8');
    }
    return Buffer.from(raw).toString('utf8');
  }

  private isAck(payload: unknown): payload is { type: 'ack' } {
    return typeof payload === 'object' && payload !== null && 'type' in payload && payload.type === 'ack';
  }
}
