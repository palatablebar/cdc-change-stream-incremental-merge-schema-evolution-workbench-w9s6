import { useEffect, useRef } from 'react';
import { useCatchupStore } from '../stores/catchupStore';

const LIVE_STATUSES = new Set(['active']);

/**
 * 追赶会话轮询：只有 active（批次在飞）时持续刷新；
 * blocked/paused/failed/completed/idle 都是可安全暂停的稳定态，停止轮询。
 */
export const useCatchupPolling = (sessionId: string | null) => {
  const refreshCurrent = useCatchupStore((state) => state.refreshCurrent);
  const current = useCatchupStore((state) => state.current);
  const timer = useRef<number | null>(null);

  const live = !!current && current.id === sessionId && LIVE_STATUSES.has(current.status);

  useEffect(() => {
    if (!sessionId || !live) return;
    let cancelled = false;
    const tick = async () => {
      await refreshCurrent();
      if (cancelled) return;
      timer.current = window.setTimeout(tick, 1500);
    };
    timer.current = window.setTimeout(tick, 1200);
    return () => {
      cancelled = true;
      if (timer.current) window.clearTimeout(timer.current);
    };
  }, [sessionId, live, refreshCurrent]);

  return { live };
};
