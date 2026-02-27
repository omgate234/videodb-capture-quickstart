import { router, protectedProcedure } from '../trpc';
import {
  StartTranscriptionInputSchema,
  StartTranscriptionOutputSchema,
} from '../../../../shared/schemas/capture.schema';
import { loadRuntimeConfig } from '../../../lib/config';
import { createChildLogger } from '../../../lib/logger';
import { connect } from 'videodb';
import type { CaptureSessionFull, RTStream } from 'videodb';

const logger = createChildLogger('transcription-procedure');

// Polling configuration (like Python meeting-copilot)
const MAX_RETRIES = 150;
const RETRY_DELAY_MS = 2000;

/**
 * Sleep helper
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Background task to start real-time transcription for a capture session.
 * Polls for RTStreams to exist (they're created after capture_session.active),
 * then calls startTranscript with the provided WebSocket connection IDs.
 *
 * This mirrors the Python meeting-copilot's start_realtime_transcription_with_ws function.
 */
async function startRealtimeTranscriptionWithWs(
  captureSessionId: string,
  apiKey: string,
  micWsConnectionId?: string,
  sysAudioWsConnectionId?: string,
  apiUrl?: string
): Promise<void> {
  try {
    // Connect to VideoDB with API key (like Python version)
    const connectOptions: { apiKey: string; baseUrl?: string } = { apiKey };
    if (apiUrl) {
      connectOptions.baseUrl = apiUrl;
    }
    const conn = connect(connectOptions);

    let mics: RTStream[] = [];
    let systemAudios: RTStream[] = [];
    const needsMic = Boolean(micWsConnectionId);
    const needsSystemAudio = Boolean(sysAudioWsConnectionId);

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      try {
        const session: CaptureSessionFull = await conn.getCaptureSession(captureSessionId);

        if (!session) {
          await sleep(RETRY_DELAY_MS);
          continue;
        }

        // Refresh to get RTStreams
        await session.refresh();

        mics = (session.rtstreams || []).filter((stream) => {
          const name = (stream.name || '').toLowerCase();
          const channelId = (stream.channelId || '').toLowerCase();
          return name === 'mic' || channelId.startsWith('mic');
        });
        systemAudios = session.getRTStream('system_audio');

        if (mics.length === 0) {
          mics = session.getRTStream('mics');
        }

        if (systemAudios.length === 0) {
          systemAudios = (session.rtstreams || []).filter((stream) => {
            const name = (stream.name || '').toLowerCase();
            const channelId = (stream.channelId || '').toLowerCase();
            return (
              name === 'system_audio' ||
              name === 'system-audio' ||
              channelId.startsWith('system_audio') ||
              channelId.startsWith('system-audio')
            );
          });
        }

        const hasRequiredMic = !needsMic || mics.length > 0;
        const hasRequiredSystemAudio = !needsSystemAudio || systemAudios.length > 0;

        if (hasRequiredMic && hasRequiredSystemAudio) {
          break;
        } else {
          await sleep(RETRY_DELAY_MS);
        }
      } catch (error) {
        logger.error({ error, sessionId: captureSessionId }, 'Transcription polling attempt failed');
        await sleep(RETRY_DELAY_MS);
      }
    }

    if ((needsMic && mics.length === 0) || (needsSystemAudio && systemAudios.length === 0)) {
      return;
    }

    // Start transcription on mic stream with WebSocket connection ID
    if (mics.length > 0 && micWsConnectionId) {
      const micStream = mics[0];
      await micStream.startTranscript(micWsConnectionId);
    }

    // Start transcription on system audio stream with WebSocket connection ID
    if (systemAudios.length > 0 && sysAudioWsConnectionId) {
      const sysStream = systemAudios[0];
      await sysStream.startTranscript(sysAudioWsConnectionId);
    }
  } catch (error) {
    logger.error({ error, sessionId: captureSessionId }, 'Failed to start transcription');
  }
}

export const transcriptionRouter = router({
  start: protectedProcedure
    .input(StartTranscriptionInputSchema)
    .output(StartTranscriptionOutputSchema)
    .mutation(async ({ input, ctx }) => {
      const { sessionId, micWsConnectionId, sysAudioWsConnectionId } = input;

      // Validate that at least one WebSocket connection ID is provided
      if (!micWsConnectionId && !sysAudioWsConnectionId) {
        return {
          status: 'skipped',
          sessionId,
          message: 'No WebSocket connection IDs provided, skipping transcription setup',
        };
      }

      // Get user's API key from context
      const apiKey = ctx.user?.apiKey;
      if (!apiKey) {
        logger.error({ sessionId }, 'No API key available for transcription');
        return {
          status: 'error',
          sessionId,
          message: 'No API key available for transcription',
        };
      }

      // Get apiUrl from runtime config if available
      const runtimeConfig = loadRuntimeConfig();
      const apiUrl = runtimeConfig.apiUrl;

      // Start transcription in background (like Python's background_tasks.add_task)
      // We don't await this - it runs in the background
      startRealtimeTranscriptionWithWs(
        sessionId,
        apiKey,
        micWsConnectionId,
        sysAudioWsConnectionId,
        apiUrl
      ).catch((error) => {
        logger.error({ error, sessionId }, 'Background transcription task failed');
      });

      return {
        status: 'started',
        sessionId,
        message: 'Transcription startup initiated. Will poll for RTStreams and start when ready.',
      };
    }),
});
