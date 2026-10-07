import { Module } from '@nestjs/common';
import { CommandsModule } from '../commands/commands.module';
import { SyncModule } from '../sync/sync.module';
import { VideosController } from './videos.controller';
import { VideosService } from './videos.service';
import { RecordingUploadService } from './recording-upload.service';

@Module({
  imports: [CommandsModule, SyncModule],
  controllers: [VideosController],
  providers: [VideosService, RecordingUploadService],
})
export class VideosModule {}
