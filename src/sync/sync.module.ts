import { Module } from '@nestjs/common';
import { SyncController } from './sync.controller';
import { SyncClientService } from './sync-client.service';
import { SyncOutboxService } from './sync-outbox.service';
import { SyncService } from './sync.service';

@Module({
  controllers: [SyncController],
  providers: [SyncService, SyncOutboxService, SyncClientService],
  exports: [SyncService, SyncOutboxService],
})
export class SyncModule {}
