import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import {
  Command,
  CommandName,
  CommandStatus,
  CommandTarget,
  Device,
  DeviceStatusValue,
  Prisma,
  Stream,
  StreamStatus,
  Video,
  VideoStatus,
} from '@prisma/client';
import {
  commandDto,
  deviceDto,
  streamDto,
  videoDto,
} from '../common/serializers';
import { PrismaService } from '../prisma/prisma.service';
import { SyncOutboxService } from '../sync/sync-outbox.service';
import type { SyncEntityType } from '../sync/sync.types';

type ClaimedCommand = Command & {
  stream: Stream | null;
  video: Video | null;
  device: Device;
};

type CommandEntityUpdate =
  | { entityType: 'command'; entityId: string; version: number; data: ReturnType<typeof commandDto> }
  | { entityType: 'device'; entityId: string; version: number; data: ReturnType<typeof deviceDto> }
  | { entityType: 'stream'; entityId: string; version: number; data: ReturnType<typeof streamDto> }
  | { entityType: 'video'; entityId: string; version: number; data: ReturnType<typeof videoDto> };

type StreamEdgeStatusPayload = {
  status?: unknown;
  streaming?: unknown;
  sourceSignalError?: unknown;
  lastError?: unknown;
  message?: unknown;
  publisher?: {
    enabled?: unknown;
    running?: unknown;
    state?: unknown;
    message?: unknown;
    lastError?: unknown;
  };
};

@Injectable()
export class CommandExecutorService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(CommandExecutorService.name);
  private readonly executorId = process.env.EDGE_DEVICE_ID?.trim() || 'edge-dev-001';
  private readonly pollIntervalMs = Number(process.env.EDGE_COMMAND_POLL_INTERVAL_MS ?? 1000);
  private readonly startVerifyAttempts = Number(process.env.STREAM_EDGE_START_VERIFY_ATTEMPTS ?? 20);
  private readonly startVerifyDelayMs = Number(process.env.STREAM_EDGE_START_VERIFY_DELAY_MS ?? 500);
  private readonly recordingsDir = process.env.EDGE_RECORDINGS_DIR?.trim() || 'recordings';
  private readonly streamEdgeBaseUrl = (
    process.env.STREAM_EDGE_CONTROL_BASE_URL?.trim() || 'http://127.0.0.1:8081'
  ).replace(/\/+$/, '');
  private timer?: NodeJS.Timeout;
  private isProcessing = false;

  constructor(
    private readonly prisma: PrismaService,
    private readonly syncOutbox: SyncOutboxService,
  ) {}

  onModuleInit() {
    this.logger.log(`Starting command executor for device ${this.executorId}`);
    this.timer = setInterval(() => {
      void this.processPendingCommands();
    }, Math.max(250, this.pollIntervalMs));
    this.timer.unref?.();
    void this.processPendingCommands();
  }

  onModuleDestroy() {
    if (this.timer) {
      clearInterval(this.timer);
    }
  }

  private async processPendingCommands() {
    if (this.isProcessing) {
      return;
    }

    this.isProcessing = true;
    try {
      while (true) {
        const command = await this.claimNextCommand();
        if (!command) {
          return;
        }
        await this.executeCommand(command);
      }
    } finally {
      this.isProcessing = false;
    }
  }

  private async claimNextCommand(): Promise<ClaimedCommand | null> {
    const pending = await this.prisma.command.findFirst({
      where: {
        deviceId: this.executorId,
        status: CommandStatus.PENDING,
      },
      orderBy: { createdAt: 'asc' },
    });

    if (!pending) {
      return null;
    }

    const now = new Date();
    const claimed = await this.prisma.$transaction(async (tx) => {
      const claim = await tx.command.updateMany({
        where: { id: pending.id, status: CommandStatus.PENDING },
        data: {
          status: CommandStatus.CLAIMED,
          claimedBy: this.executorId,
          claimedAt: now,
          version: { increment: 1 },
          updatedAt: now,
        },
      });

      if (claim.count !== 1) {
        return null;
      }

      const command = await tx.command.findUniqueOrThrow({
        where: { id: pending.id },
        include: {
          stream: true,
          video: true,
          device: true,
        },
      });

      await this.emitSyncEvent(
        tx,
        'command',
        command.id,
        command.version,
        commandDto(command),
        now,
      );

      return command;
    });

    return claimed;
  }

  private async executeCommand(command: ClaimedCommand) {
    this.logger.log(`Executing ${command.command} ${command.target} command ${command.id}`);
    await this.markCommandProcessing(command.id);

    try {
      switch (command.target) {
        case CommandTarget.STREAM:
          if (command.command === CommandName.START) {
            await this.startStream(command);
          } else {
            await this.stopStream(command);
          }
          break;
        case CommandTarget.RECORDING:
          if (command.command === CommandName.START) {
            await this.startRecording(command);
          } else {
            await this.stopRecording(command);
          }
          break;
      }
    } catch (error) {
      await this.failCommand(command, error);
    }
  }

  private async markCommandProcessing(commandId: string) {
    const now = new Date();
    await this.prisma.$transaction(async (tx) => {
      const command = await tx.command.update({
        where: { id: commandId },
        data: {
          status: CommandStatus.PROCESSING,
          version: { increment: 1 },
          updatedAt: now,
        },
      });
      await this.emitSyncEvent(tx, 'command', command.id, command.version, commandDto(command), now);
    });
  }

  private async startStream(command: ClaimedCommand) {
    if (!command.streamId) {
      throw new Error('START STREAM command is missing stream_id');
    }

    const now = new Date();
    await this.runStreamControlHook('EDGE_START_STREAM_COMMAND', '/stream/start', command);
    await this.verifyStreamEdgeStarted();

    await this.prisma.$transaction(async (tx) => {
      const device = await tx.device.update({
        where: { id: command.deviceId },
        data: {
          status: DeviceStatusValue.STREAMING,
          sourceReady: true,
          streaming: true,
          lastError: null,
          lastSeenAt: now,
          version: { increment: 1 },
          updatedAt: now,
        },
      });
      const stream = await tx.stream.update({
        where: { id: command.streamId! },
        data: {
          status: StreamStatus.LIVE,
          mediasoupStreamId: command.streamId,
          playbackUrl: this.viewerUrl(command.streamId!),
          startedAt: command.stream?.startedAt ?? now,
          lastError: null,
          version: { increment: 1 },
          updatedAt: now,
        },
      });
      const updatedCommand = await this.completeCommand(tx, command.id, 'Stream started on edge device', now);

      await this.emitSyncEvents(tx, now, [
        { entityType: 'device', entityId: device.id, version: device.version, data: deviceDto(device) },
        { entityType: 'stream', entityId: stream.id, version: stream.version, data: streamDto(stream) },
        {
          entityType: 'command',
          entityId: updatedCommand.id,
          version: updatedCommand.version,
          data: commandDto(updatedCommand),
        },
      ]);
    });
  }

  private async stopStream(command: ClaimedCommand) {
    if (!command.streamId) {
      throw new Error('STOP STREAM command is missing stream_id');
    }

    const now = new Date();
    await this.runStreamControlHook('EDGE_STOP_STREAM_COMMAND', '/stream/stop', command);

    await this.prisma.$transaction(async (tx) => {
      const activeRecordings = await tx.video.count({
        where: {
          deviceId: command.deviceId,
          status: VideoStatus.RECORDING,
        },
      });
      const device = await tx.device.update({
        where: { id: command.deviceId },
        data: {
          status: DeviceStatusValue.ONLINE,
          streaming: false,
          recording: activeRecordings > 0,
          lastError: null,
          lastSeenAt: now,
          version: { increment: 1 },
          updatedAt: now,
        },
      });
      const stream = await tx.stream.update({
        where: { id: command.streamId! },
        data: {
          status: StreamStatus.ENDED,
          endedAt: now,
          lastError: null,
          version: { increment: 1 },
          updatedAt: now,
        },
      });
      const updatedCommand = await this.completeCommand(tx, command.id, 'Stream stopped on edge device', now);

      await this.emitSyncEvents(tx, now, [
        { entityType: 'device', entityId: device.id, version: device.version, data: deviceDto(device) },
        { entityType: 'stream', entityId: stream.id, version: stream.version, data: streamDto(stream) },
        {
          entityType: 'command',
          entityId: updatedCommand.id,
          version: updatedCommand.version,
          data: commandDto(updatedCommand),
        },
      ]);
    });
  }

  private async startRecording(command: ClaimedCommand) {
    if (!command.videoId) {
      throw new Error('START RECORDING command is missing video_id');
    }
    const videoId = command.videoId;

    const now = new Date();
    const hookResponse = await this.runStreamControlHook(
      'EDGE_START_RECORDING_COMMAND',
      '/record/start',
      command,
    );
    const recordingPath = this.recordingPathFromHookResponse(hookResponse) ?? this.localRecordingPath(videoId);

    await this.prisma.$transaction(async (tx) => {
      const device = await tx.device.update({
        where: { id: command.deviceId },
        data: {
          recording: true,
          lastError: null,
          lastSeenAt: now,
          version: { increment: 1 },
          updatedAt: now,
        },
      });
      const video = await tx.video.update({
        where: { id: videoId },
        data: {
          status: VideoStatus.RECORDING,
          edgeLocalPath: recordingPath,
          startedAt: command.video?.startedAt ?? now,
          version: { increment: 1 },
          updatedAt: now,
        },
      });
      const updatedCommand = await this.completeCommand(tx, command.id, 'Recording started on edge device', now);

      await this.emitSyncEvents(tx, now, [
        { entityType: 'device', entityId: device.id, version: device.version, data: deviceDto(device) },
        { entityType: 'video', entityId: video.id, version: video.version, data: videoDto(video) },
        {
          entityType: 'command',
          entityId: updatedCommand.id,
          version: updatedCommand.version,
          data: commandDto(updatedCommand),
        },
      ]);
    });
  }

  private async stopRecording(command: ClaimedCommand) {
    if (!command.videoId) {
      throw new Error('STOP RECORDING command is missing video_id');
    }
    const videoId = command.videoId;

    const now = new Date();
    const hookResponse = await this.runStreamControlHook(
      'EDGE_STOP_RECORDING_COMMAND',
      '/record/stop',
      command,
    );
    const recordingPath =
      this.recordingPathFromHookResponse(hookResponse) ??
      command.video?.edgeLocalPath ??
      this.localRecordingPath(videoId);

    await this.prisma.$transaction(async (tx) => {
      const video = await tx.video.update({
        where: { id: videoId },
        data: {
          status: VideoStatus.UPLOADING,
          edgeLocalPath: recordingPath,
          endedAt: now,
          durationSeconds: this.durationSeconds(command.video?.startedAt, now),
          version: { increment: 1 },
          updatedAt: now,
        },
      });
      const activeRecordings = await tx.video.count({
        where: {
          deviceId: command.deviceId,
          status: VideoStatus.RECORDING,
          id: { not: videoId },
        },
      });
      const device = await tx.device.update({
        where: { id: command.deviceId },
        data: {
          recording: activeRecordings > 0,
          lastError: null,
          lastSeenAt: now,
          version: { increment: 1 },
          updatedAt: now,
        },
      });
      const updatedCommand = await this.completeCommand(tx, command.id, 'Recording stopped and queued for upload', now);

      await this.emitSyncEvents(tx, now, [
        { entityType: 'device', entityId: device.id, version: device.version, data: deviceDto(device) },
        { entityType: 'video', entityId: video.id, version: video.version, data: videoDto(video) },
        {
          entityType: 'command',
          entityId: updatedCommand.id,
          version: updatedCommand.version,
          data: commandDto(updatedCommand),
        },
      ]);
    });
  }

  private async failCommand(command: ClaimedCommand, error: unknown) {
    const message = error instanceof Error ? error.message : 'Command execution failed';
    this.logger.warn(`Command ${command.id} failed: ${message}`);
    const now = new Date();

    await this.prisma.$transaction(async (tx) => {
      const device = await tx.device.update({
        where: { id: command.deviceId },
        data: {
          status: DeviceStatusValue.ERROR,
          streaming:
            command.target === CommandTarget.STREAM && command.command === CommandName.START
              ? false
              : undefined,
          recording:
            command.target === CommandTarget.RECORDING && command.command === CommandName.START
              ? false
              : undefined,
          lastError: message,
          lastSeenAt: now,
          version: { increment: 1 },
          updatedAt: now,
        },
      });
      const updatedCommand = await tx.command.update({
        where: { id: command.id },
        data: {
          status: CommandStatus.FAILED,
          errorMessage: message,
          processedAt: now,
          version: { increment: 1 },
          updatedAt: now,
        },
      });

      const updates: CommandEntityUpdate[] = [
        { entityType: 'device', entityId: device.id, version: device.version, data: deviceDto(device) },
        {
          entityType: 'command',
          entityId: updatedCommand.id,
          version: updatedCommand.version,
          data: commandDto(updatedCommand),
        },
      ];

      if (command.streamId) {
        const stream = await tx.stream.update({
          where: { id: command.streamId },
          data: {
            status: StreamStatus.ERROR,
            lastError: message,
            version: { increment: 1 },
            updatedAt: now,
          },
        });
        updates.push({
          entityType: 'stream',
          entityId: stream.id,
          version: stream.version,
          data: streamDto(stream),
        });
      }

      if (command.videoId) {
        const video = await tx.video.update({
          where: { id: command.videoId },
          data: {
            status: VideoStatus.FAILED,
            version: { increment: 1 },
            updatedAt: now,
          },
        });
        updates.push({
          entityType: 'video',
          entityId: video.id,
          version: video.version,
          data: videoDto(video),
        });
      }

      await this.emitSyncEvents(tx, now, updates);
    });
  }

  private async completeCommand(
    tx: Prisma.TransactionClient,
    commandId: string,
    resultMessage: string,
    processedAt: Date,
  ) {
    return tx.command.update({
      where: { id: commandId },
      data: {
        status: CommandStatus.COMPLETED,
        resultMessage,
        errorMessage: null,
        processedAt,
        version: { increment: 1 },
        updatedAt: processedAt,
      },
    });
  }

  private async emitSyncEvents(
    tx: Prisma.TransactionClient,
    occurredAt: Date,
    updates: CommandEntityUpdate[],
  ) {
    for (const update of updates) {
      await this.emitSyncEvent(
        tx,
        update.entityType,
        update.entityId,
        update.version,
        update.data,
        occurredAt,
      );
    }
  }

  private async emitSyncEvent(
    tx: Prisma.TransactionClient,
    entityType: SyncEntityType,
    entityId: string,
    version: number,
    data: Record<string, unknown>,
    occurredAt: Date,
  ) {
    await this.syncOutbox.createEvent(
      {
        entityType,
        entityId,
        action: 'update',
        version,
        occurredAt,
        data,
      },
      tx,
    );
  }

  private async runStreamControlHook(
    envName: string,
    defaultPath: string,
    command: ClaimedCommand,
  ): Promise<unknown> {
    const url = process.env[`${envName}_URL`]?.trim() || `${this.streamEdgeBaseUrl}${defaultPath}`;
    const caseId = command.videoId ?? command.streamId ?? command.id;

    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        command_id: command.id,
        case_id: caseId,
        command: command.command.toLowerCase(),
        target: command.target.toLowerCase(),
        device_id: command.deviceId,
        stream_id: command.streamId,
        video_id: command.videoId,
        metadata_managed_by_client: true,
      }),
    });

    if (!response.ok) {
      const body = await response.text();
      throw new Error(
        `${envName}_URL failed with status ${response.status}${body ? `: ${body}` : ''}`,
      );
    }

    const contentType = response.headers.get('content-type') ?? '';
    if (!contentType.includes('application/json')) {
      return undefined;
    }

    return response.json();
  }

  private async verifyStreamEdgeStarted() {
    const attempts = Math.max(1, this.startVerifyAttempts);
    const delayMs = Math.max(0, this.startVerifyDelayMs);
    let lastStatus: StreamEdgeStatusPayload | undefined;

    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      if (attempt > 1 && delayMs > 0) {
        await this.delay(delayMs);
      }

      lastStatus = await this.fetchStreamEdgeStatus();
      if (this.streamEdgeIsLive(lastStatus)) {
        return;
      }

      if (lastStatus.status === 'error') {
        throw new Error(this.streamEdgeStatusMessage(lastStatus));
      }
    }

    throw new Error(this.streamEdgeStatusMessage(lastStatus));
  }

  private async fetchStreamEdgeStatus(): Promise<StreamEdgeStatusPayload> {
    const response = await fetch(`${this.streamEdgeBaseUrl}/stream/status`);
    if (!response.ok) {
      const body = await response.text();
      throw new Error(
        `STREAM_EDGE_CONTROL_BASE_URL status check failed with status ${response.status}${
          body ? `: ${body}` : ''
        }`,
      );
    }

    const payload = await response.json();
    return payload && typeof payload === 'object' ? (payload as StreamEdgeStatusPayload) : {};
  }

  private streamEdgeStatusMessage(status?: StreamEdgeStatusPayload) {
    const detail =
      this.optionalStatusString(status?.lastError) ??
      this.optionalStatusString(status?.sourceSignalError) ??
      this.optionalStatusString(status?.publisher?.lastError) ??
      this.optionalStatusString(status?.publisher?.message) ??
      this.optionalStatusString(status?.message);

    if (detail) {
      return `Stream edge did not enter streaming state: ${detail}`;
    }

    return 'Stream edge did not enter streaming state';
  }

  private streamEdgeIsLive(status: StreamEdgeStatusPayload) {
    if (status.streaming !== true || status.status === 'error') {
      return false;
    }

    const publisher = status.publisher;
    if (!publisher || typeof publisher !== 'object') {
      return true;
    }

    if (publisher.enabled === false) {
      return false;
    }

    if (publisher.state === 'failed') {
      return false;
    }

    if (publisher.enabled === true && publisher.running !== true) {
      return false;
    }

    return true;
  }

  private optionalStatusString(value: unknown) {
    return typeof value === 'string' && value.trim() ? value.trim() : undefined;
  }

  private delay(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private viewerUrl(streamId: string) {
    const baseUrl = process.env.MEDIA_SERVER_PUBLIC_URL?.trim();
    if (!baseUrl) {
      return null;
    }
    const url = new URL(baseUrl);
    url.searchParams.set('streamId', streamId);
    return url.toString();
  }

  private localRecordingPath(videoId: string) {
    return `${this.recordingsDir.replace(/\/+$/, '')}/${videoId}.mp4`;
  }

  private recordingPathFromHookResponse(value: unknown) {
    if (!value || typeof value !== 'object') {
      return undefined;
    }

    const payload = value as {
      recordingStatus?: {
        output_path?: unknown;
        local_path?: unknown;
      };
    };

    const outputPath = payload.recordingStatus?.output_path ?? payload.recordingStatus?.local_path;
    return typeof outputPath === 'string' && outputPath.trim() ? outputPath : undefined;
  }

  private durationSeconds(startedAt: Date | null | undefined, endedAt: Date) {
    if (!startedAt) {
      return undefined;
    }
    return Math.max(0, Math.round((endedAt.getTime() - startedAt.getTime()) / 1000));
  }
}
