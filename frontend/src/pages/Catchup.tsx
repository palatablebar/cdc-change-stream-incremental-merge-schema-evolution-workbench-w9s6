import { useEffect, useMemo, useState } from 'react';
import { MetricStrip } from '../components/common/MetricStrip';
import { PanelHead } from '../components/common/PanelHead';
import { StatusPill } from '../components/common/StatusPill';
import { EmptyState } from '../components/common/EmptyState';
import { RequireRole } from '../components/common/RequireRole';
import { CATCHUP_RECOVERY_KIND, CATCHUP_STATUS } from '../constants/cdc';
import { useCatchupPolling } from '../hooks/useCatchupPolling';
import { useAuthStore } from '../stores/authStore';
import { useCatchupStore } from '../stores/catchupStore';
import { useStreamStore } from '../stores/streamStore';
import { formatCount, formatLsn, formatShortTime } from '../utils/format';
import type { CatchupSession } from '../types/models';

/** 连续增量追赶：按批追最新位点，批次确认才推进安全位点，遇空洞停在其前。 */
export const CatchupPage = () => {
  const board = useStreamStore((state) => state.board);
  const selectedStreamId = useStreamStore((state) => state.selectedStreamId);
  const selectStream = useStreamStore((state) => state.selectStream);
  const loadBoard = useStreamStore((state) => state.loadBoard);

  const current = useCatchupStore((state) => state.current);
  const sessions = useCatchupStore((state) => state.sessions);
  const loading = useCatchupStore((state) => state.loading);
  const acting = useCatchupStore((state) => state.acting);
  const error = useCatchupStore((state) => state.error);
  const loadList = useCatchupStore((state) => state.loadList);
  const loadForStream = useCatchupStore((state) => state.loadForStream);
  const start = useCatchupStore((state) => state.start);
  const resume = useCatchupStore((state) => state.resume);
  const pause = useCatchupStore((state) => state.pause);
  const fillGap = useCatchupStore((state) => state.fillGap);

  const canTrigger = useAuthStore((state) => state.can('catchup:trigger'));
  const role = useAuthStore((state) => state.user?.role);

  const [batchSize, setBatchSize] = useState('20');
  const [gapPk, setGapPk] = useState('');
  const [gapAfter, setGapAfter] = useState('');

  const stream = useMemo(
    () => board?.streams.find((item) => item.id === selectedStreamId) ?? board?.streams[0] ?? null,
    [board, selectedStreamId],
  );

  useEffect(() => {
    void loadBoard();
    void loadList();
  }, [loadBoard, loadList]);

  useEffect(() => {
    if (stream) void loadForStream(stream.id);
  }, [stream?.id, loadForStream]);

  useCatchupPolling(current?.id ?? null);

  const session: CatchupSession | null = current && stream && current.stream_id === stream.id ? current : null;
  const live = session?.status === 'active';

  const onStart = async () => {
    if (!stream) return;
    const size = Math.max(1, Number(batchSize) || 20);
    await start(stream.id, size);
    void loadList();
  };

  const onResume = async () => {
    if (!session) return;
    await resume(session.id);
    void loadList();
  };

  const onPause = async () => {
    if (!session) return;
    await pause(session.id);
  };

  const onFillGap = async () => {
    if (!session || session.gap_lsn === null) return;
    let after: Record<string, unknown> | null = null;
    if (gapAfter.trim()) {
      try {
        after = JSON.parse(gapAfter);
      } catch {
        after = null;
      }
    }
    await fillGap(session.id, {
      gap_lsn: session.gap_lsn,
      op: 'insert',
      pk_value: gapPk.trim() || null,
      after,
    });
    setGapPk('');
    setGapAfter('');
    void loadList();
  };

  const recoveryKind = session?.last_recovery?.kind;
  const recoveryText = recoveryKind ? CATCHUP_RECOVERY_KIND[recoveryKind] ?? recoveryKind : null;

  // MetricStrip 只接受 default/amber/danger/ok，状态徽标的 muted 归到 default
  const metricTone = (tone: string): 'default' | 'amber' | 'danger' | 'ok' =>
    tone === 'muted' ? 'default' : (tone as 'amber' | 'danger' | 'ok');

  return (
    <div>
      <PanelHead
        title="连续增量追赶"
        sub={stream ? `${stream.name} · 按可控批量追最新位点，每批确认后才推进安全位点` : '选择变更流'}
        actions={
          <select
            className="select"
            value={stream?.id ?? ''}
            onChange={(event) => selectStream(event.target.value)}
          >
            {(board?.streams ?? []).map((item) => (
              <option key={item.id} value={item.id}>
                {item.name}
              </option>
            ))}
          </select>
        }
      />

      {error ? <div className="notice danger">{error}</div> : null}

      {!stream ? (
        <EmptyState title="还没有可用的变更流" hint="先在「变更流看板」登记一条流并接入事件。" />
      ) : (
        <>
          <MetricStrip
            items={[
              {
                label: '安全位点 LSN',
                value: session ? session.safe_lsn_display : formatLsn(stream.watermark_lsn),
                note: '最后已确认接纳位置',
                tone: 'ok',
              },
              {
                label: '剩余积压',
                value: session ? formatCount(session.backlog_events) : formatCount(stream.lag),
                note: '安全位点之后的事件',
                tone: (session?.backlog_events ?? stream.lag) > 0 ? 'amber' : 'ok',
              },
              {
                label: '当前批次',
                value: session?.current_run ? `#${session.current_batch_no}` : '—',
                note: session?.current_run
                  ? `窗口 ${session.current_run.window_start_lsn}→${session.current_run.window_end_lsn}`
                  : '无在飞批次',
              },
              {
                label: '追赶状态',
                value: session ? session.status_label : '未开始',
                note: live ? '批次在飞，自动推进' : '稳定态，可安全暂停',
                tone: session ? metricTone(CATCHUP_STATUS[session.status].tone) : 'default',
              },
            ]}
          />

          {/* 发起区 */}
          {!session ? (
            <div className="section">
              <div className="section-label"><span>发起追赶</span></div>
              <div className="panel">
                <div className="panel-body">
                  <div className="form-row">
                    <label className="field">
                      <span className="field-label">每批事件位点数（可控批量，1-500）</span>
                      <input
                        className="input"
                        type="number"
                        min={1}
                        max={500}
                        value={batchSize}
                        onChange={(event) => setBatchSize(event.target.value)}
                        style={{ width: 220 }}
                      />
                    </label>
                    <RequireRole
                      permission="catchup:trigger"
                      fallback={
                        <div className="notice" style={{ flex: 1 }}>
                          当前角色 <span className="mono">{role}</span> 只读，发起追赶需要 admin 或 engineer。
                        </div>
                      }
                    >
                      <button className="btn primary" disabled={acting || loading} onClick={() => void onStart()}>
                        {acting ? '提交中…' : '开始连续追赶'}
                      </button>
                    </RequireRole>
                  </div>
                  <p className="tiny faint" style={{ marginTop: 10 }}>
                    从安全位点的下一个 LSN 开始，按批量取连续位点；遇到空洞会停在空洞之前，补齐后再继续。
                    每批归并成功才推进水位线，失败不推进，续跑从最后确认位置重新规划，重复执行不会跳过事件。
                  </p>
                </div>
              </div>
            </div>
          ) : (
            <>
              {/* 会话状态与控制 */}
              <div className="section">
                <div className="section-label"><span>追赶会话</span></div>
                <div className="panel">
                  <div className="panel-body">
                    <div className="form-row" style={{ alignItems: 'center' }}>
                      <StatusPill
                        label={session.status_label}
                        tone={CATCHUP_STATUS[session.status].tone}
                        running={CATCHUP_STATUS[session.status].running}
                      />
                      <span className="tiny faint mono">
                        批次 {session.succeeded_batches}/{session.total_batches} 成功
                        {session.failed_batches > 0 ? ` · ${session.failed_batches} 失败` : ''}
                      </span>
                      {session.pause_requested ? (
                        <span className="pill amber" title="当前批次确认后会停在安全位点">
                          暂停待生效
                        </span>
                      ) : null}
                      <span style={{ flex: 1 }} />
                      <RequireRole
                        permission="catchup:trigger"
                        fallback={<span className="tiny faint">只读角色不可操作</span>}
                      >
                        {session.status === 'active' ? (
                          <button className="btn" disabled={acting || session.pause_requested} onClick={() => void onPause()}>
                            {session.pause_requested ? '本批后暂停…' : '跑完本批后暂停'}
                          </button>
                        ) : (
                          <button className="btn primary" disabled={acting} onClick={() => void onResume()}>
                            {session.status === 'failed' ? '从确认位点续跑' : '继续追赶'}
                          </button>
                        )}
                      </RequireRole>
                    </div>

                    <table className="table" style={{ marginTop: 14 }}>
                      <tbody>
                        <tr>
                          <td className="tiny faint" style={{ width: 160 }}>安全位点（已确认）</td>
                          <td className="mono">{session.safe_lsn_display}</td>
                          <td className="tiny faint" style={{ width: 160 }}>最新位点</td>
                          <td className="mono">{session.max_lsn_display}</td>
                        </tr>
                        <tr>
                          <td className="tiny faint">剩余积压事件</td>
                          <td className="mono" style={{ color: session.backlog_events > 0 ? 'var(--color-warn)' : undefined }}>
                            {formatCount(session.backlog_events)}
                          </td>
                          <td className="tiny faint">空洞位点</td>
                          <td className="mono" style={{ color: session.gap_lsn !== null ? 'var(--color-danger)' : undefined }}>
                            {session.gap_lsn_display ?? '无'}
                          </td>
                        </tr>
                        <tr>
                          <td className="tiny faint">当前批次窗口</td>
                          <td className="mono">
                            {session.current_run
                              ? `#${session.current_run.batch_no} [${session.current_run.window_start_lsn}, ${session.current_run.window_end_lsn}] · ${session.current_run.status}`
                              : '—'}
                          </td>
                          <td className="tiny faint">最近进展时间</td>
                          <td className="tiny">{formatShortTime(session.last_progress_at)}</td>
                        </tr>
                      </tbody>
                    </table>

                    {session.last_error ? (
                      <div className="notice danger" style={{ marginTop: 12 }}>
                        最近错误：{session.last_error}
                      </div>
                    ) : null}
                  </div>
                </div>
              </div>

              {/* 空洞补齐 */}
              {session.status === 'blocked' && session.gap_lsn !== null ? (
                <div className="section">
                  <div className="section-label"><span>补齐位点空洞 LSN {session.gap_lsn}</span></div>
                  <div className="panel">
                    <div className="panel-body">
                      <p className="tiny faint" style={{ marginTop: 0 }}>
                        安全位点 {session.safe_lsn_display} 之后的 LSN {session.gap_lsn} 事件未到达，追赶已停在它之前。
                        补齐该位点后会自动继续，不会跳过其后的事件。
                      </p>
                      <div className="form-row">
                        <label className="field">
                          <span className="field-label">主键值（pk_value）</span>
                          <input
                            className="input"
                            value={gapPk}
                            onChange={(event) => setGapPk(event.target.value)}
                            placeholder={`PK-${session.gap_lsn}`}
                            style={{ width: 220 }}
                          />
                        </label>
                        <label className="field" style={{ flex: 1 }}>
                          <span className="field-label">after 载荷（可选 JSON）</span>
                          <input
                            className="input"
                            value={gapAfter}
                            onChange={(event) => setGapAfter(event.target.value)}
                            placeholder='{"order_id":"PK-x","amount":100}'
                          />
                        </label>
                        <RequireRole
                          permission="catchup:trigger"
                          fallback={<span className="tiny faint">只读角色不可补洞</span>}
                        >
                          <button className="btn primary" disabled={acting} onClick={() => void onFillGap()}>
                            补齐并继续
                          </button>
                        </RequireRole>
                      </div>
                    </div>
                  </div>
                </div>
              ) : null}

              {/* 最近一次恢复结果 */}
              <div className="section">
                <div className="section-label"><span>最近一次恢复结果</span></div>
                <div className="panel">
                  <div className="panel-body tiny">
                    {session.last_recovery ? (
                      <>
                        <div className="form-row" style={{ alignItems: 'center' }}>
                          <span className={`pill ${CATCHUP_STATUS[session.status].tone}`}>
                            {recoveryText}
                          </span>
                          <span className="faint">{formatShortTime(session.last_recovery.at)}</span>
                          <span style={{ flex: 1 }} />
                          <span className="mono faint">当时安全位点 {formatLsn(session.last_recovery.safe_lsn)}</span>
                        </div>
                        <pre className="sql-block" style={{ marginTop: 10, marginBottom: 0 }}>
                          {JSON.stringify(session.last_recovery.detail, null, 2)}
                        </pre>
                      </>
                    ) : (
                      <span className="faint">暂无恢复记录</span>
                    )}
                  </div>
                </div>
              </div>
            </>
          )}

          {/* 历史会话 */}
          <div className="section">
            <div className="section-label"><span>追赶会话记录</span></div>
            {sessions.length === 0 ? (
              <EmptyState title="还没有追赶会话" hint="选择批量后发起第一次连续追赶。" />
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>流</th>
                    <th>状态</th>
                    <th className="num">批量</th>
                    <th className="num">安全位点</th>
                    <th className="num">空洞</th>
                    <th className="num">积压</th>
                    <th className="num">批次(成/败)</th>
                    <th>最近进展</th>
                  </tr>
                </thead>
                <tbody>
                  {sessions.map((item) => (
                    <tr
                      key={item.id}
                      className="row-click"
                      onClick={() => item.stream_id && selectStream(item.stream_id)}
                    >
                      <td className="mono tiny">{item.stream_name}</td>
                      <td>
                        <StatusPill
                          label={item.status_label}
                          tone={CATCHUP_STATUS[item.status].tone}
                          running={CATCHUP_STATUS[item.status].running}
                        />
                      </td>
                      <td className="num">{item.batch_size}</td>
                      <td className="num mono">{item.safe_lsn_display}</td>
                      <td className="num mono" style={{ color: item.gap_lsn !== null ? 'var(--color-danger)' : undefined }}>
                        {item.gap_lsn_display ?? '—'}
                      </td>
                      <td className="num">{formatCount(item.backlog_events)}</td>
                      <td className="num">
                        {item.succeeded_batches}/{item.total_batches}
                        {item.failed_batches > 0 ? <span className="tiny faint"> ({item.failed_batches}败)</span> : null}
                      </td>
                      <td className="tiny faint">{formatShortTime(item.last_progress_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </>
      )}
    </div>
  );
};
