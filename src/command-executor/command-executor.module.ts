import { Module } from '@nestjs/common';
import { SyncModule } from '../sync/sync.module';
import { CommandExecutorService } from './command-executor.service';

@Module({
  imports: [SyncModule],
  providers: [CommandExecutorService],
})
export class CommandExecutorModule {}
