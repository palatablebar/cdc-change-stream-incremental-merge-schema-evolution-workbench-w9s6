import { create } from 'zustand';
import { catchupApi, type GapFillPayload } from '../api/catchup';
import { errorMessage } from '../utils/errorMessage';
import type { CatchupSession } from '../types/models';

interface CatchupState {
  sessions: CatchupSession[];
  current: CatchupSession | null;
  loading: boolean;
  acting: boolean;
  error: string | null;
  loadList: () => Promise<void>;
  loadForStream: (streamId: string) => Promise<CatchupSession | null>;
  refreshCurrent: () => Promise<void>;
  start: (streamId: string, batchSize: number) => Promise<CatchupSession | null>;
  resume: (id: string) => Promise<void>;
  pause: (id: string) => Promise<void>;
  fillGap: (id: string, payload: GapFillPayload) => Promise<void>;
  clearError: () => void;
}

export const useCatchupStore = create<CatchupState>((set, get) => ({
  sessions: [],
  current: null,
  loading: false,
  acting: false,
  error: null,

  loadList: async () => {
    set({ loading: true, error: null });
    try {
      const res = await catchupApi.list();
      set({ sessions: res.items, loading: false });
    } catch (error) {
      set({ loading: false, error: errorMessage(error) });
    }
  },

  loadForStream: async (streamId) => {
    set({ loading: true, error: null });
    try {
      const res = await catchupApi.byStream(streamId);
      set({ current: res.session, loading: false });
      return res.session;
    } catch (error) {
      set({ loading: false, error: errorMessage(error) });
      return null;
    }
  },

  refreshCurrent: async () => {
    const current = get().current;
    if (!current) return;
    try {
      const next = await catchupApi.get(current.id);
      set({ current: next, error: null });
    } catch (error) {
      set({ error: errorMessage(error) });
    }
  },

  start: async (streamId, batchSize) => {
    set({ acting: true, error: null });
    try {
      const session = await catchupApi.start(streamId, batchSize);
      set({ current: session, acting: false });
      return session;
    } catch (error) {
      set({ acting: false, error: errorMessage(error) });
      return null;
    }
  },

  resume: async (id) => {
    set({ acting: true, error: null });
    try {
      const session = await catchupApi.resume(id);
      set({ current: session, acting: false });
    } catch (error) {
      set({ acting: false, error: errorMessage(error) });
    }
  },

  pause: async (id) => {
    set({ acting: true, error: null });
    try {
      const session = await catchupApi.pause(id);
      set({ current: session, acting: false });
    } catch (error) {
      set({ acting: false, error: errorMessage(error) });
    }
  },

  fillGap: async (id, payload) => {
    set({ acting: true, error: null });
    try {
      const session = await catchupApi.fillGap(id, payload);
      set({ current: session, acting: false });
    } catch (error) {
      set({ acting: false, error: errorMessage(error) });
    }
  },

  clearError: () => set({ error: null }),
}));
