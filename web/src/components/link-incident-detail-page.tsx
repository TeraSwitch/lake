import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useParams, useNavigate, useLocation, Link } from 'react-router-dom'
import { ArrowLeft, ExternalLink } from 'lucide-react'
import {
  LinkHealthTimeline,
  LinkPacketLossChart,
  LinkInterfaceIssuesChart,
  LinkLatencyChart,
  LinkJitterChart,
  LinkTrafficChart,
} from '@/components/link-charts'
import {
  fetchLinkIncidentDetail,
  fetchLinkMetrics,
  type LinkIncidentDetailResponse,
  type IncidentEventV2,
  type IncidentSeverity,
  type IncidentStatus,
  type EntityStatusChange,
} from '@/lib/api'
import { useDocumentTitle } from '@/hooks/use-document-title'

function formatDuration(seconds: number | undefined): string {
  if (seconds === undefined) return '-'
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`
  if (seconds < 86400) {
    const hours = Math.floor(seconds / 3600)
    const mins = Math.floor((seconds % 3600) / 60)
    return mins > 0 ? `${hours}h ${mins}m` : `${hours}h`
  }
  const days = Math.floor(seconds / 86400)
  const hours = Math.floor((seconds % 86400) / 3600)
  const mins = Math.floor((seconds % 3600) / 60)
  const parts = [`${days}d`]
  if (hours > 0) parts.push(`${hours}h`)
  if (mins > 0 && days < 7) parts.push(`${mins}m`)
  return parts.join(' ')
}

function formatTimeAgo(isoString: string): string {
  if (isoString === 'unknown') return 'Unknown'
  const date = new Date(isoString)
  if (isNaN(date.getTime()) || date.getFullYear() < 2000) return 'Unknown'
  const now = new Date()
  const diffMs = now.getTime() - date.getTime()
  const diffSecs = Math.floor(diffMs / 1000)

  if (diffSecs < 60) return `${diffSecs}s ago`
  if (diffSecs < 3600) return `${Math.floor(diffSecs / 60)}m ago`

  const days = Math.floor(diffSecs / 86400)
  const hours = Math.floor((diffSecs % 86400) / 3600)
  const minutes = Math.floor((diffSecs % 3600) / 60)

  if (days > 0) {
    const parts = [`${days}d`]
    if (hours > 0) parts.push(`${hours}h`)
    if (minutes > 0 && days < 7) parts.push(`${minutes}m`)
    return `${parts.join(' ')} ago`
  }
  const parts = [`${hours}h`]
  if (minutes > 0) parts.push(`${minutes}m`)
  return `${parts.join(' ')} ago`
}

function formatTimestamp(isoString: string): string {
  if (isoString === 'unknown') return 'Unknown'
  const date = new Date(isoString)
  if (isNaN(date.getTime()) || date.getFullYear() < 2000) return 'Unknown'
  return date.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function formatOffsetFromStart(startTs: string, eventTs: string): string {
  const start = new Date(startTs).getTime()
  const event = new Date(eventTs).getTime()
  const diffSecs = Math.max(0, Math.floor((event - start) / 1000))
  if (diffSecs === 0) return '+0m'
  if (diffSecs < 3600) return `+${Math.floor(diffSecs / 60)}m`
  const hours = Math.floor(diffSecs / 3600)
  const mins = Math.floor((diffSecs % 3600) / 60)
  return mins > 0 ? `+${hours}h ${mins}m` : `+${hours}h`
}

function SeverityBadge({ severity }: { severity: IncidentSeverity }) {
  const config: Record<IncidentSeverity, { label: string; className: string }> = {
    critical: {
      label: 'critical',
      className: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200',
    },
    warning: {
      label: 'warning',
      className: 'bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-200',
    },
  }
  const c = config[severity]
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${c.className}`}>
      {c.label}
    </span>
  )
}

function IncidentTypeBadge({ type }: { type: string }) {
  const config: Record<string, { label: string; className: string }> = {
    packet_loss: {
      label: 'packet loss',
      className: 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200',
    },
    errors: {
      label: 'errors',
      className: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200',
    },
    discards: {
      label: 'discards',
      className: 'bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-200',
    },
    fcs: {
      label: 'fcs errors',
      className: 'bg-pink-100 text-pink-800 dark:bg-pink-900 dark:text-pink-200',
    },
    carrier: {
      label: 'carrier transitions',
      className: 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200',
    },
    no_latency_data: {
      label: 'no latency data',
      className: 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200',
    },
    no_traffic_data: {
      label: 'no traffic data',
      className: 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200',
    },
    isis_down: {
      label: 'ISIS down',
      className: 'bg-rose-100 text-rose-800 dark:bg-rose-900 dark:text-rose-200',
    },
    isis_overload: {
      label: 'ISIS overload',
      className: 'bg-rose-100 text-rose-800 dark:bg-rose-900 dark:text-rose-200',
    },
    isis_unreachable: {
      label: 'ISIS unreachable',
      className: 'bg-rose-100 text-rose-800 dark:bg-rose-900 dark:text-rose-200',
    },
  }
  const c = config[type] || { label: type, className: 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-200' }
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${c.className}`}>
      {c.label}
    </span>
  )
}

function StatusBadge({ status }: { status: IncidentStatus }) {
  const config: Record<IncidentStatus, { label: string; dotClass: string }> = {
    ongoing: { label: 'Ongoing', dotClass: 'bg-green-500' },
    pending_resolution: { label: 'Pending Resolution', dotClass: 'bg-amber-500' },
    resolved: { label: 'Resolved', dotClass: 'bg-gray-400' },
  }
  const c = config[status]
  return (
    <span className="inline-flex items-center gap-1.5 text-sm">
      <span className={`inline-flex rounded-full h-2 w-2 ${c.dotClass}`} />
      {c.label}
    </span>
  )
}

type TimelineEntry =
  | { kind: 'event'; ts: string; event: IncidentEventV2 }
  | { kind: 'status_change'; ts: string; change: EntityStatusChange }

const eventTypeConfig: Record<string, { label: string; dotClass: string }> = {
  opened: { label: 'Incident Opened', dotClass: 'bg-green-500' },
  symptom_added: { label: 'Symptom Added', dotClass: 'bg-blue-500' },
  symptom_resolved: { label: 'Symptom Resolved', dotClass: 'bg-amber-500' },
  resolved: { label: 'Incident Resolved', dotClass: 'bg-gray-400' },
}

function statusChangeDotClass(newStatus: string): string {
  if (newStatus === 'activated' || newStatus === 'active') return 'bg-green-500'
  return 'bg-slate-400'
}

function TimelineEntryRow({ entry, isLast, startedAt }: { entry: TimelineEntry; isLast: boolean; startedAt: string }) {
  if (entry.kind === 'status_change') {
    const dotClass = statusChangeDotClass(entry.change.new_status)
    return (
      <div className="relative flex gap-4">
        <div className="flex flex-col items-center">
          <div className={`w-3 h-3 shrink-0 mt-1 rotate-45 ${dotClass}`} />
          {!isLast && <div className="w-px flex-1 bg-border min-h-[24px]" />}
        </div>
        <div className="pb-5 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-medium text-sm text-muted-foreground">
              Status: {entry.change.previous_status} → {entry.change.new_status}
            </span>
            <span className="text-xs font-mono text-muted-foreground/70">
              {formatOffsetFromStart(startedAt, entry.ts)}
            </span>
            <span className="text-xs text-muted-foreground">
              {formatTimeAgo(entry.ts)}
            </span>
            <span className="text-xs text-muted-foreground/50">
              ({formatTimestamp(entry.ts)})
            </span>
          </div>
        </div>
      </div>
    )
  }

  const cfg = eventTypeConfig[entry.event.event_type] || { label: entry.event.event_type, dotClass: 'bg-gray-400' }
  return (
    <div className="relative flex gap-4">
      <div className="flex flex-col items-center">
        <div className={`w-3 h-3 rounded-full shrink-0 mt-1 ${cfg.dotClass}`} />
        {!isLast && <div className="w-px flex-1 bg-border min-h-[24px]" />}
      </div>
      <div className="pb-5 min-w-0">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="font-medium text-sm">{cfg.label}</span>
          <span className="text-xs font-mono text-muted-foreground/70">
            {formatOffsetFromStart(startedAt, entry.ts)}
          </span>
          <span className="text-xs text-muted-foreground">
            {formatTimeAgo(entry.ts)}
          </span>
          <span className="text-xs text-muted-foreground/50">
            ({formatTimestamp(entry.ts)})
          </span>
        </div>
        <div className="mt-1.5 flex items-center gap-2 flex-wrap">
          {entry.event.active_symptoms.length > 0 && (
            <div className="flex items-center gap-1 flex-wrap">
              <span className="text-xs text-muted-foreground mr-1">Symptoms:</span>
              {entry.event.active_symptoms.map((s) => (
                <IncidentTypeBadge key={s} type={s} />
              ))}
            </div>
          )}
          <span className="text-xs text-muted-foreground">
            Severity: <SeverityBadge severity={entry.event.severity} />
          </span>
        </div>
      </div>
    </div>
  )
}

const COLLAPSE_THRESHOLD = 4

function EventTimeline({
  events,
  statusChanges,
  startedAt,
}: {
  events: IncidentEventV2[]
  statusChanges: EntityStatusChange[]
  startedAt: string
}) {
  const [expanded, setExpanded] = useState(false)

  const entries: TimelineEntry[] = useMemo(() => {
    const all = [
      ...events.map((e) => ({ kind: 'event' as const, ts: e.event_ts, event: e })),
      ...(statusChanges || []).map((c) => ({ kind: 'status_change' as const, ts: c.changed_ts, change: c })),
    ]
    all.sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime())
    return all
  }, [events, statusChanges])

  if (entries.length <= COLLAPSE_THRESHOLD) {
    return (
      <div className="relative">
        {entries.map((entry, i) => (
          <TimelineEntryRow key={`${entry.kind}-${i}`} entry={entry} isLast={i === entries.length - 1} startedAt={startedAt} />
        ))}
      </div>
    )
  }

  // Collapsible: show summary by default, full timeline on expand
  const transitionCount = entries.filter(
    (e) => e.kind === 'event' && (e.event.event_type === 'symptom_added' || e.event.event_type === 'symptom_resolved'),
  ).length
  const firstTs = entries[entries.length - 1]?.ts
  const lastTs = entries[0]?.ts
  const spanMs = firstTs && lastTs ? new Date(lastTs).getTime() - new Date(firstTs).getTime() : 0
  const spanLabel = formatDuration(Math.floor(spanMs / 1000))
  const allSymptoms = Array.from(new Set(
    entries.flatMap((e) => e.kind === 'event' ? e.event.active_symptoms : [])
  ))

  if (!expanded) {
    return (
      <button
        onClick={() => setExpanded(true)}
        className="w-full text-left text-sm text-muted-foreground hover:text-foreground transition-colors py-2 px-3 rounded-md border border-border hover:border-foreground/20"
      >
        <div className="flex items-center gap-2 flex-wrap">
          <span className="font-medium text-foreground">{transitionCount} symptom transitions</span>
          <span>·</span>
          <span>{entries.length} events over {spanLabel}</span>
          {allSymptoms.map((s) => (
            <IncidentTypeBadge key={s} type={s} />
          ))}
          <span className="text-xs ml-auto">Show timeline →</span>
        </div>
      </button>
    )
  }

  return (
    <div>
      <button
        onClick={() => setExpanded(false)}
        className="text-xs text-muted-foreground hover:text-foreground mb-3 transition-colors"
      >
        ← Collapse timeline
      </button>
      <div className="relative">
        {entries.map((entry, i) => (
          <TimelineEntryRow key={`${entry.kind}-${i}`} entry={entry} isLast={i === entries.length - 1} startedAt={startedAt} />
        ))}
      </div>
    </div>
  )
}

function LinkIncidentDetailContent({ data }: { data: LinkIncidentDetailResponse }) {
  const isOngoing = data.status === 'ongoing' || data.status === 'pending_resolution'

  // Fetch unified metrics for the incident time range
  const startTime = useMemo(() => Math.floor(new Date(data.started_at).getTime() / 1000), [data.started_at])
  const endTime = useMemo(() => !isOngoing && data.ended_at ? Math.floor(new Date(data.ended_at).getTime() / 1000) : Math.floor(Date.now() / 1000), [isOngoing, data.ended_at])

  const { data: metrics } = useQuery({
    queryKey: ['linkMetrics', data.link_pk, startTime, endTime],
    queryFn: () => fetchLinkMetrics(data.link_pk, { startTime, endTime }),
    refetchInterval: isOngoing ? 60000 : undefined,
  })

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <div className="flex items-center gap-2 mb-2">
          <SeverityBadge severity={data.severity} />
          <StatusBadge status={data.status} />
        </div>
        <div className="flex items-center gap-2">
          <h1 className="text-xl font-semibold">
            Link: {data.link_code}
          </h1>
          <Link
            to={`/dz/links/${encodeURIComponent(data.link_pk)}`}
            className="text-primary hover:underline inline-flex items-center gap-0.5"
          >
            <ExternalLink className="h-4 w-4" />
          </Link>
        </div>
        <div className="text-sm text-muted-foreground mt-1">
          {data.contributor_code} · {data.link_type} · <span className="font-mono">{data.side_a_metro} ↔ {data.side_z_metro}</span>
        </div>
      </div>

      {/* Timing */}
      <div className="grid grid-cols-3 gap-4 text-sm">
        <div>
          <div className="text-muted-foreground text-xs mb-1">Started</div>
          <div>{formatTimeAgo(data.started_at)}</div>
          <div className="text-xs text-muted-foreground/50">{formatTimestamp(data.started_at)}</div>
        </div>
        <div>
          <div className="text-muted-foreground text-xs mb-1">Ended</div>
          {isOngoing ? (
            <div className="text-muted-foreground">Ongoing</div>
          ) : (
            <>
              <div>{formatTimeAgo(data.ended_at!)}</div>
              <div className="text-xs text-muted-foreground/50">{formatTimestamp(data.ended_at!)}</div>
            </>
          )}
        </div>
        <div>
          <div className="text-muted-foreground text-xs mb-1">Duration</div>
          <div>
            {isOngoing
              ? formatDuration(Math.floor((Date.now() - new Date(data.started_at).getTime()) / 1000))
              : formatDuration(data.duration_seconds)
            }
          </div>
        </div>
      </div>

      {/* Event Timeline */}
      {(data.events.length > 0 || (data.status_changes || []).length > 0) && (
        <div>
          <h2 className="text-sm font-semibold mb-3">Event Timeline</h2>
          <EventTimeline
            events={data.events}
            statusChanges={data.status_changes}
            startedAt={data.started_at}
          />
        </div>
      )}

      {/* Metrics Charts */}
      {metrics && (
        <div className="space-y-4">
          <LinkHealthTimeline data={metrics} />
          <LinkPacketLossChart data={metrics} className="rounded-lg border border-border p-4" />
          <LinkInterfaceIssuesChart data={metrics} className="rounded-lg border border-border p-4" />
          <LinkLatencyChart data={metrics} className="rounded-lg border border-border p-4" />
          <LinkJitterChart data={metrics} className="rounded-lg border border-border p-4" />
          <LinkTrafficChart data={metrics} className="rounded-lg border border-border p-4" />
        </div>
      )}
    </div>
  )
}

export function LinkIncidentDetailPage() {
  const { id } = useParams<{ id: string }>()
  const navigate = useNavigate()
  const location = useLocation()

  const { data, isLoading, error } = useQuery({
    queryKey: ['linkIncidentDetail', id],
    queryFn: () => fetchLinkIncidentDetail(id!),
    enabled: !!id,
    retry: (_, error) => (error as Error).message !== 'Incident not found',
  })

  useDocumentTitle(data ? `Incident - ${data.link_code}` : 'Incident Detail')

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-4xl mx-auto px-4 sm:px-8 py-8">
        <Link
          to="/incidents/links"
          onClick={(e) => {
            if (location.key !== 'default' && !e.metaKey && !e.ctrlKey) {
              e.preventDefault()
              navigate(-1)
            }
          }}
          className="inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground mb-6 transition-colors"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to incidents
        </Link>

        {isLoading && (
          <div className="space-y-4">
            <div className="animate-pulse bg-muted rounded h-8 w-64" />
            <div className="animate-pulse bg-muted rounded h-4 w-48" />
            <div className="animate-pulse bg-muted rounded h-32 w-full" />
          </div>
        )}

        {error && !data && (
          <div className="flex flex-col items-center justify-center py-12 text-center border border-border rounded-lg">
            {(error as Error).message === 'Incident not found' ? (
              <>
                <h3 className="text-lg font-medium mb-2">Incident not found</h3>
                <p className="text-sm text-muted-foreground">
                  This incident may have been resolved and removed, or the link is invalid.
                </p>
              </>
            ) : (
              <>
                <h3 className="text-lg font-medium mb-2">Unable to load incident</h3>
                <p className="text-sm text-muted-foreground">
                  {(error as Error).message || 'Something went wrong.'}
                </p>
              </>
            )}
          </div>
        )}

        {data && <LinkIncidentDetailContent data={data} />}
      </div>
    </div>
  )
}
