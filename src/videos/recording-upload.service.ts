import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { createReadStream } from 'node:fs';
import { appendFile, stat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { VideoStatus } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class RecordingUploadService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RecordingUploadService.name);
  private timer?: NodeJS.Timeout;
  private busy = false;
  constructor(private readonly prisma: PrismaService) {}
  onModuleInit() { this.timer = setInterval(() => void this.flush(), 5000); }
  onModuleDestroy() { if (this.timer) clearInterval(this.timer); }
  private async flush() {
    if (this.busy) return;
    this.busy = true;
    try {
      const videos = await this.prisma.video.findMany({ where: {
        status: VideoStatus.UPLOADING, deviceId: process.env.EDGE_DEVICE_ID || 'edge-dev-001',
        edgeLocalPath: { not: null }, endedAt: { not: null },
        frameCount: { not: null },
      }, take: 5 });
      for (const video of videos) {
        try {
          const info = await stat(video.edgeLocalPath!);
          if (!info.size) throw new Error('Recording is empty');
          const hashStarted = process.hrtime.bigint();
          const hash = createHash('sha256');
          for await (const chunk of createReadStream(video.edgeLocalPath!)) hash.update(chunk);
          const hashDurationMs = Number(process.hrtime.bigint() - hashStarted) / 1e6;
          const base = (process.env.CLOUD_API_URL || 'http://127.0.0.1:3000').replace(/\/$/, '');
          const requestStarted = process.hrtime.bigint();
          const utcStartMs = Date.now();
          const response = await fetch(`${base}/videos/${video.id}/file`, {
            method: 'PUT', headers: { Authorization: `Bearer ${process.env.EDGE_SYNC_TOKEN}`,
              'Content-Type': 'video/mp4', 'Content-Length': String(info.size), 'X-Content-SHA256': hash.digest('hex') },
            body: createReadStream(video.edgeLocalPath!), duplex: 'half', signal: AbortSignal.timeout(300000),
          } as unknown as RequestInit);
          if (!response.ok) throw new Error(`Upload returned ${response.status}: ${await response.text()}`);
          await response.arrayBuffer();
          if (process.env.THESIS_TRANSFER_TIMING_FILE) {
            const sample = { video_id: video.id, size_bytes: info.size, utc_start_ms: utcStartMs,
              utc_end_ms: Date.now(), hash_duration_ms: hashDurationMs,
              request_duration_ms: Number(process.hrtime.bigint() - requestStarted) / 1e6 };
            await appendFile(process.env.THESIS_TRANSFER_TIMING_FILE, JSON.stringify(sample) + '\n', { mode: 0o600 })
              .catch(error => this.logger.warn(`Could not save transfer timing: ${String(error)}`));
          }
          this.logger.log(`Uploaded recording ${video.id} (${info.size} bytes); waiting for cloud metadata sync`);
        } catch (error) { this.logger.warn(error instanceof Error ? error.message : 'Recording upload failed'); }
      }
    } catch (error) { this.logger.warn(error instanceof Error ? error.message : 'Upload poll failed'); }
    finally { this.busy = false; }
  }
}
