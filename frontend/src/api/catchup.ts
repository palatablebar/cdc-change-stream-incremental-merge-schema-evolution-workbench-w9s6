import type { CatchupSession, StreamCatchup } from '../types/models';
import { api } from './client';

export interface GapFillPayload {
  gap_lsn: number;
  op: 'insert' | 'update' | 'delete';
  pk_value?: string | null;
  after?: Record<string, unknown> | null;
  before?: Record<string, unknown> | null;
}

export const catchupApi = {
  list: (limit = 30) => api.get<{ items: CatchupSession[]; total: number }>(`/catchup/sessions?limit=${limit}`),
  get: (id: string) => api.get<CatchupSession>(`/catchup/sessions/${id}`),
  byStream: (streamId: string) =>
    api.get<StreamCatchup>(`/catchup/streams/${encodeURIComponent(streamId)}/session`),
  start: (streamId: string, batchSize: number) =>
    api.post<CatchupSession>('/catchup/sessions', { stream_id: streamId, batch_size: batchSize }),
  resume: (id: string) => api.post<CatchupSession>(`/catchup/sessions/${id}/resume`),
  pause: (id: string) => api.post<CatchupSession>(`/catchup/sessions/${id}/pause`),
  fillGap: (id: string, payload: GapFillPayload) =>
    api.post<CatchupSession>(`/catchup/sessions/${id}/fill-gap`, payload),
};
