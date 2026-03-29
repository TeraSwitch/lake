import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useState, useMemo, useEffect } from 'react'
import { useSearchParams, useNavigate, useLocation, Link } from 'react-router-dom'
import { ShieldAlert, ExternalLink, ChevronDown, RefreshCw } from 'lucide-react'
import { cn } from '@/lib/utils'
import {
  fetchLinkIncidentsV2,
  fetchDeviceIncidentsV2,
  type LinkIncidentV2,
  type DrainedLinkInfoV2,
  type DeviceIncidentV2,
  type DrainedDeviceInfoV2,
  type IncidentTimeRange,
  type FetchIncidentsV2Params,
  type LinkIncidentsV2Response,
  type DeviceIncidentsV2Response,
  type IncidentSeverity,
} from '@/lib/api'
import { StatusFilters, useStatusFilters } from '@/components/status-search-bar'
import { PageHeader } from '@/components/page-header'

const timeRanges: { value: IncidentTimeRange; label: string }[] = [
  { value: '3h', label: '3h' },
  { value: '6h', label: '6h' },
  { value: '12h', label: '12h' },
  { value: '24h', label: '24h' },
  { value: '3d', label: '3d' },
  { value: '7d', label: '7d' },
  { value: '30d', label: '30d' },
]

function RelativeTime({ timestamp }: { timestamp: number }) {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 10000)
    return () => clearInterval(id)
  }, [])
  const seconds = Math.floor((now - timestamp) / 1000)
  if (seconds < 5) return <>just now</>
  if (seconds < 60) return <>{seconds}s ago</>
  const minutes = Math.floor(seconds / 60)
  return <>{minutes}m ago</>
}

function Skeleton({ className }: { className?: string }) {
  return <div className={`animate-pulse bg-muted rounded ${className || ''}`} />
}

function IncidentsContentSkeleton() {
  return (
    <>
      <div className="grid grid-cols-3 sm:grid-cols-5 gap-3 mb-6">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-[72px]" />
        ))}
      </div>
      <Skeleton className="h-[400px] rounded-lg" />
    </>
  )
}

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


type IncidentScope = 'links' | 'devices'

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

function IncidentSection({
  title,
  description,
  count,
  defaultOpen = true,
  children,
}: {
  title: string
  description: string
  count: number
  defaultOpen?: boolean
  children: React.ReactNode
}) {
  const [open, setOpen] = useState(defaultOpen)

  return (
    <div className="border border-border rounded-lg bg-card">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-muted/30 transition-colors rounded-t-lg"
      >
        <ChevronDown
          className={cn(
            'h-4 w-4 shrink-0 text-muted-foreground transition-transform',
            !open && '-rotate-90'
          )}
        />
        <h2 className="text-sm font-semibold">{title}</h2>
        <span className="px-1.5 py-0.5 text-xs rounded-full bg-muted-foreground/10 text-muted-foreground tabular-nums">
          {count}
        </span>
        <span className="text-xs text-muted-foreground">{description}</span>
      </button>
      {open && count > 0 && (
        <div className="px-4 pb-4 overflow-hidden">
          {children}
        </div>
      )}
    </div>
  )
}

function DrainedBadge() {
  return (
    <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-slate-100 text-slate-800 dark:bg-slate-800 dark:text-slate-200">
      drained
    </span>
  )
}


function ReadinessDot({ readiness }: { readiness: string }) {
  const colors: Record<string, string> = {
    red: 'bg-red-500',
    yellow: 'bg-yellow-500',
    green: 'bg-green-500',
    gray: 'bg-gray-400',
  }
  const labels: Record<string, string> = {
    red: 'Active issues',
    yellow: 'Recently clear',
    green: 'Clear 30m+',
    gray: 'No issues in range',
  }
  return (
    <span className="inline-flex items-center gap-1.5" title={labels[readiness] || readiness}>
      <span className={`inline-flex rounded-full h-2.5 w-2.5 ${colors[readiness] || 'bg-gray-400'}`} />
      <span className="text-xs text-muted-foreground">{labels[readiness]}</span>
    </span>
  )
}

function dedupeSymptoms(incidents: { symptoms: string[] }[]): string[] {
  const seen = new Set<string>()
  const result: string[] = []
  for (const inc of incidents) {
    for (const s of inc.symptoms) {
      if (!seen.has(s)) {
        seen.add(s)
        result.push(s)
      }
    }
  }
  return result
}

export function IncidentsPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const navigate = useNavigate()
  const location = useLocation()

  // Scope from path
  const scope: IncidentScope = location.pathname === '/incidents/devices' ? 'devices' : 'links'

  // Parse URL params with defaults
  const range = (searchParams.get('range') as IncidentTimeRange) || '24h'
  const typeParam = searchParams.get('type') || ''
  const selectedTypes = useMemo(() => {
    if (!typeParam || typeParam === 'all') return new Set<string>()
    return new Set(typeParam.split(',').filter(Boolean))
  }, [typeParam])
  const view = (searchParams.get('view') as 'active' | 'drained') || 'active'
  const filterParam = searchParams.get('filter') || ''
  const severityParam = (searchParams.get('severity') as 'all' | IncidentSeverity) || 'all'
  const showTransient = searchParams.get('transient') === 'true'

  const toggleType = (t: string) => {
    const next = new Set(selectedTypes)
    if (next.has(t)) {
      next.delete(t)
    } else {
      next.add(t)
    }
    updateParams({ type: next.size === 0 ? undefined : Array.from(next).join(',') })
  }

  const filters = useStatusFilters()

  const updateParams = (updates: Record<string, string | undefined>) => {
    const newParams = new URLSearchParams(searchParams)
    for (const [key, value] of Object.entries(updates)) {
      if (value && value !== getDefaultValue(key)) {
        newParams.set(key, value)
      } else {
        newParams.delete(key)
      }
    }
    setSearchParams(newParams)
  }

  const getDefaultValue = (key: string): string => {
    switch (key) {
      case 'range': return '24h'
      case 'type': return ''
      case 'view': return 'active'
      case 'severity': return 'all'
      default: return ''
    }
  }

  // Fetch all incidents and filter client-side so stat cards always show
  // unfiltered counts. Severity and symptom filters are applied in the UI.
  const apiParams: FetchIncidentsV2Params = useMemo(() => ({
    range,
    filter: filterParam || undefined,
  }), [range, filterParam])

  const linkQuery = useQuery({
    queryKey: ['linkIncidentsV2', range, filterParam],
    queryFn: () => fetchLinkIncidentsV2(apiParams),
    refetchInterval: 60000,
    placeholderData: keepPreviousData,
    enabled: scope === 'links',
  })

  const deviceQuery = useQuery({
    queryKey: ['deviceIncidentsV2', range, filterParam],
    queryFn: () => fetchDeviceIncidentsV2(apiParams),
    refetchInterval: 60000,
    placeholderData: keepPreviousData,
    enabled: scope === 'devices',
  })

  const activeQuery = scope === 'links' ? linkQuery : deviceQuery
  const isLoading = activeQuery.isLoading
  const isFetching = activeQuery.isFetching
  const error = activeQuery.error
  const hasData = activeQuery.data != null
  const dataUpdatedAt = activeQuery.dataUpdatedAt
  const linkData = linkQuery.data as LinkIncidentsV2Response | undefined
  const deviceData = deviceQuery.data as DeviceIncidentsV2Response | undefined

  // Unfiltered summaries for the stat cards
  const allDrainedSummary = scope === 'links'
    ? linkData?.drained_summary || { total: 0, with_incidents: 0, ready: 0, not_ready: 0 }
    : deviceData?.drained_summary || { total: 0, with_incidents: 0, ready: 0, not_ready: 0 }

  const rawSummary = scope === 'links'
    ? linkData?.summary || { total: 0, ongoing: 0, critical: 0, warning: 0, by_symptom: {} }
    : deviceData?.summary || { total: 0, ongoing: 0, critical: 0, warning: 0, by_symptom: {} }

  // Recompute summary excluding transient incidents when hidden.
  const summary = useMemo(() => {
    if (showTransient) return rawSummary
    const allIncidents = scope === 'links' ? (linkData?.incidents || []) : (deviceData?.incidents || [])
    const nonTransient = allIncidents.filter(i => i.status !== 'resolved' || i.duration_seconds >= 1800)
    const s = { total: nonTransient.length, ongoing: 0, critical: 0, warning: 0, by_symptom: {} as Record<string, number> }
    for (const i of nonTransient) {
      if (i.status === 'ongoing' || i.status === 'pending_resolution') s.ongoing++
      if (i.severity === 'critical') s.critical++
      else s.warning++
      for (const sym of i.symptoms) s.by_symptom[sym] = (s.by_symptom[sym] || 0) + 1
    }
    return s
  }, [rawSummary, showTransient, scope, linkData?.incidents, deviceData?.incidents])

  // Client-side filtering (severity + symptom type)
  const hasTypeFilter = selectedTypes.size > 0
  const hasSeverityFilter = severityParam !== 'all'

  const filterIncident = <T extends { severity: string; symptoms: string[] }>(i: T): boolean => {
    if (hasSeverityFilter && i.severity !== severityParam) return false
    if (hasTypeFilter && !i.symptoms.some(s => selectedTypes.has(s))) return false
    return true
  }

  const linkIncidents = useMemo(() => {
    const all = linkData?.incidents || []
    if (!hasTypeFilter && !hasSeverityFilter) return all
    return all.filter(filterIncident)
  }, [linkData?.incidents, hasTypeFilter, hasSeverityFilter, selectedTypes, severityParam]) // eslint-disable-line react-hooks/exhaustive-deps

  const deviceIncidents = useMemo(() => {
    const all = deviceData?.incidents || []
    if (!hasTypeFilter && !hasSeverityFilter) return all
    return all.filter(filterIncident)
  }, [deviceData?.incidents, hasTypeFilter, hasSeverityFilter, selectedTypes, severityParam]) // eslint-disable-line react-hooks/exhaustive-deps

  const drainedLinks = useMemo(() => {
    const all = linkData?.drained || []
    if (!hasTypeFilter && !hasSeverityFilter) return all
    return all.map(dl => ({
      ...dl,
      active_incidents: dl.active_incidents.filter(filterIncident),
      recent_incidents: dl.recent_incidents.filter(filterIncident),
    })).filter(dl => dl.active_incidents.length > 0 || dl.recent_incidents.length > 0)
  }, [linkData?.drained, hasTypeFilter, hasSeverityFilter, selectedTypes, severityParam]) // eslint-disable-line react-hooks/exhaustive-deps

  const drainedDevices = useMemo(() => {
    const all = deviceData?.drained || []
    if (!hasTypeFilter && !hasSeverityFilter) return all
    return all.map(dd => ({
      ...dd,
      active_incidents: dd.active_incidents.filter(filterIncident),
      recent_incidents: dd.recent_incidents.filter(filterIncident),
    })).filter(dd => dd.active_incidents.length > 0 || dd.recent_incidents.length > 0)
  }, [deviceData?.drained, hasTypeFilter, hasSeverityFilter, selectedTypes, severityParam]) // eslint-disable-line react-hooks/exhaustive-deps

  // Sort state
  type SortField = 'started_at' | 'ended_at' | 'duration'
  const [sortField, setSortField] = useState<SortField>('started_at')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')

  const sortIncidentList = <T extends { started_at: string; status: string; duration_seconds: number }>(items: T[]): T[] => {
    return [...items].sort((a, b) => {
      const aOngoing = a.status === 'ongoing' || a.status === 'pending_resolution'
      const bOngoing = b.status === 'ongoing' || b.status === 'pending_resolution'
      if (sortField === 'started_at') {
        const aTime = new Date(a.started_at).getTime()
        const bTime = new Date(b.started_at).getTime()
        return sortDir === 'asc' ? aTime - bTime : bTime - aTime
      } else if (sortField === 'ended_at') {
        const aEnd = aOngoing ? Infinity : new Date(a.started_at).getTime() + a.duration_seconds * 1000
        const bEnd = bOngoing ? Infinity : new Date(b.started_at).getTime() + b.duration_seconds * 1000
        return sortDir === 'asc' ? aEnd - bEnd : bEnd - aEnd
      } else {
        const aDur = aOngoing ? Infinity : a.duration_seconds
        const bDur = bOngoing ? Infinity : b.duration_seconds
        return sortDir === 'asc' ? aDur - bDur : bDur - aDur
      }
    })
  }

  // Split by v2 status: ongoing, pending_resolution, resolved
  // Within resolved, split by duration: recent (< 30min) vs sustained (>= 30min)
  const splitByStatusV2 = <T extends { status: string; started_at: string; duration_seconds: number }>(items: T[]) => {
    const ongoing: T[] = []
    const pendingResolution: T[] = []
    const resolvedRecent: T[] = []
    const resolvedSustained: T[] = []
    for (const i of items) {
      switch (i.status) {
        case 'ongoing': ongoing.push(i); break
        case 'pending_resolution': pendingResolution.push(i); break
        case 'resolved': {
          if (i.duration_seconds < 1800) {
            resolvedRecent.push(i)
          } else {
            resolvedSustained.push(i)
          }
          break
        }
      }
    }
    return {
      ongoing: sortIncidentList(ongoing),
      pendingResolution: sortIncidentList(pendingResolution),
      resolvedRecent: sortIncidentList(resolvedRecent),
      resolvedSustained: sortIncidentList(resolvedSustained),
    }
  }

  const linkIncidentsByStatus = useMemo(
    () => splitByStatusV2(linkIncidents),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [linkIncidents, sortField, sortDir],
  )

  const deviceIncidentsByStatus = useMemo(
    () => splitByStatusV2(deviceIncidents),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [deviceIncidents, sortField, sortDir],
  )

  // Symptom cards use summary.by_symptom from the API
  const symptomCards = useMemo(() => {
    if (scope === 'links') {
      return [
        { key: 'packet_loss', label: 'Packet Loss' },
        { key: 'errors', label: 'Errors' },
        { key: 'fcs', label: 'FCS Errors' },
        { key: 'discards', label: 'Discards' },
        { key: 'carrier', label: 'Carrier Transitions' },
        { key: 'no_latency_data', label: 'No Latency Data' },
        { key: 'no_traffic_data', label: 'No Traffic Data' },
        { key: 'isis_down', label: 'ISIS Down' },
      ]
    }
    return [
      { key: 'errors', label: 'Errors' },
      { key: 'fcs', label: 'FCS Errors' },
      { key: 'discards', label: 'Discards' },
      { key: 'carrier', label: 'Carrier Transitions' },
      { key: 'no_latency_data', label: 'No Latency Data' },
      { key: 'no_traffic_data', label: 'No Traffic Data' },
      { key: 'isis_overload', label: 'ISIS Overload' },
      { key: 'isis_unreachable', label: 'ISIS Unreachable' },
    ]
  }, [scope])

  const toggleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc')
    } else {
      setSortField(field)
      setSortDir('desc')
    }
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-6xl mx-auto px-4 sm:px-8 py-8">
        <PageHeader
          icon={ShieldAlert}
          title="Incidents"
        />

        {/* Scope toggle */}
        <div className="flex items-center gap-2 mb-4">
          <div className="flex items-center gap-1 bg-muted rounded-md p-1">
            <button
              onClick={() => navigate('/incidents/links')}
              className={`px-3 py-1 text-sm rounded transition-colors ${
                scope === 'links'
                  ? 'bg-background text-foreground shadow-sm'
                  : 'text-muted-foreground hover:text-foreground'
              }`}
            >
              Links
            </button>
            <button
              onClick={() => navigate('/incidents/devices')}
              className={`px-3 py-1 text-sm rounded transition-colors ${
                scope === 'devices'
                  ? 'bg-background text-foreground shadow-sm'
                  : 'text-muted-foreground hover:text-foreground'
              }`}
            >
              Devices
            </button>
          </div>
        </div>

        {/* Filters row */}
        <div className="flex flex-wrap items-center gap-4 mb-4">
          {/* Time range */}
          <div className="flex items-center gap-1 bg-muted rounded-md p-1">
            {timeRanges.map((tr) => (
              <button
                key={tr.value}
                onClick={() => updateParams({ range: tr.value })}
                className={`px-3 py-1 text-sm rounded transition-colors ${
                  range === tr.value
                    ? 'bg-background text-foreground shadow-sm'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                {tr.label}
              </button>
            ))}
          </div>

          {/* Severity filter */}
          <div className="flex items-center gap-1 bg-muted rounded-md p-1">
            {([
              { value: 'all' as const, label: 'All' },
              { value: 'critical' as const, label: 'Critical' },
              { value: 'warning' as const, label: 'Warning' },
            ]).map((sev) => (
              <button
                key={sev.value}
                onClick={() => updateParams({ severity: sev.value })}
                className={`px-3 py-1 text-sm rounded transition-colors ${
                  severityParam === sev.value
                    ? 'bg-background text-foreground shadow-sm'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                {sev.label}
              </button>
            ))}
          </div>

          <StatusFilters />
        </div>

        {isLoading ? <IncidentsContentSkeleton /> : error && !hasData ? (
          <div className="flex flex-col items-center justify-center py-12 text-center border border-border rounded-lg">
            <ShieldAlert className="h-12 w-12 text-muted-foreground mb-4" />
            <h3 className="text-lg font-medium mb-2">Unable to load incidents</h3>
            <p className="text-sm text-muted-foreground mb-4">
              {(error as Error).message || 'Something went wrong. The API server may be unavailable.'}
            </p>
            <button
              onClick={() => activeQuery.refetch()}
              className="px-4 py-2 text-sm border border-border rounded-md hover:bg-muted transition-colors"
            >
              Retry
            </button>
          </div>
        ) : (<>
        {/* Data freshness indicator */}
        <div className="flex items-center gap-2 mb-4 text-xs text-muted-foreground">
          <button
            onClick={() => activeQuery.refetch()}
            className="inline-flex items-center gap-1.5 hover:text-foreground transition-colors"
            title="Refresh now"
          >
            <RefreshCw className={cn('h-3 w-3', isFetching && 'animate-spin')} />
            {dataUpdatedAt ? (
              <span>Updated <RelativeTime timestamp={dataUpdatedAt} /></span>
            ) : (
              <span>Refreshing...</span>
            )}
          </button>
          {error && (
            <span className="text-yellow-600 dark:text-yellow-400">· Refresh failed, showing previous data</span>
          )}
        </div>

        {/* Severity + symptom stat cards */}
        <div className="grid gap-3 mb-4 grid-cols-2">
          <button
            onClick={() => updateParams({ severity: severityParam === 'critical' ? 'all' : 'critical' })}
            className={`text-center p-3 rounded-lg border transition-colors ${
              severityParam === 'critical'
                ? 'border-red-500 bg-red-500/5 ring-1 ring-red-500'
                : 'border-border hover:border-muted-foreground/30'
            }`}
          >
            <div className="text-2xl font-medium tabular-nums tracking-tight text-red-600 dark:text-red-400">
              {summary.critical}
            </div>
            <div className="text-xs text-muted-foreground">Critical</div>
          </button>
          <button
            onClick={() => updateParams({ severity: severityParam === 'warning' ? 'all' : 'warning' })}
            className={`text-center p-3 rounded-lg border transition-colors ${
              severityParam === 'warning'
                ? 'border-amber-500 bg-amber-500/5 ring-1 ring-amber-500'
                : 'border-border hover:border-muted-foreground/30'
            }`}
          >
            <div className="text-2xl font-medium tabular-nums tracking-tight text-amber-600 dark:text-amber-400">
              {summary.warning}
            </div>
            <div className="text-xs text-muted-foreground">Warning</div>
          </button>
        </div>

        {/* Symptom filter cards */}
        <div className={`grid gap-3 mb-6 grid-cols-4 sm:grid-cols-${symptomCards.length}`}>
          {symptomCards.map(({ key, label }) => {
            const count = summary.by_symptom[key] || 0
            const isSelected = selectedTypes.has(key)
            return (
              <button
                key={key}
                onClick={() => toggleType(key)}
                className={`text-center p-3 rounded-lg border transition-colors ${
                  isSelected
                    ? 'border-primary bg-primary/5 ring-1 ring-primary'
                    : 'border-border hover:border-muted-foreground/30'
                }`}
              >
                <div className="text-2xl font-medium tabular-nums tracking-tight">
                  {count}
                </div>
                <div className="text-xs text-muted-foreground">{label}</div>
              </button>
            )
          })}
        </div>

        {/* View tabs + transient toggle */}
        <div className="flex items-center gap-4 mb-6">
        <div className="flex items-center gap-1 bg-muted rounded-md p-1 w-fit">
          <button
            onClick={() => updateParams({ view: 'active' })}
            className={`px-4 py-1.5 text-sm rounded transition-colors ${
              view === 'active'
                ? 'bg-background text-foreground shadow-sm'
                : 'text-muted-foreground hover:text-foreground'
            }`}
          >
            Activated
            {summary.ongoing > 0 && (
              <span className="ml-1.5 px-1.5 py-0.5 text-xs rounded-full bg-red-500/10 text-red-600 dark:text-red-400">
                {summary.ongoing}
              </span>
            )}
          </button>
          <button
            onClick={() => updateParams({ view: 'drained' })}
            className={`px-4 py-1.5 text-sm rounded transition-colors ${
              view === 'drained'
                ? 'bg-background text-foreground shadow-sm'
                : 'text-muted-foreground hover:text-foreground'
            }`}
          >
            Drained
            {allDrainedSummary.total > 0 && (
              <span className="ml-1.5 px-1.5 py-0.5 text-xs rounded-full bg-muted-foreground/10 text-muted-foreground">
                {allDrainedSummary.total}
              </span>
            )}
          </button>
        </div>

          <label className="flex items-center gap-2 cursor-pointer select-none">
            <button
              role="switch"
              aria-checked={showTransient}
              onClick={() => updateParams({ transient: showTransient ? undefined : 'true' })}
              className={`relative inline-flex h-5 w-9 shrink-0 rounded-full border-2 border-transparent transition-colors ${
                showTransient ? 'bg-primary' : 'bg-muted-foreground/30'
              }`}
            >
              <span
                className={`pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow-sm ring-0 transition-transform ${
                  showTransient ? 'translate-x-4' : 'translate-x-0'
                }`}
              />
            </button>
            <span className="text-sm text-muted-foreground">Show transient</span>
          </label>
        </div>

        {/* Active view */}
        {view === 'active' && (
          <>
            {(() => {
              const isEmpty = scope === 'links' ? linkIncidents.length === 0 : deviceIncidents.length === 0
              if (isEmpty) {
                return (
                  <div className="flex flex-col items-center justify-center py-12 text-center border border-border rounded-lg">
                    <ShieldAlert className="h-12 w-12 text-muted-foreground mb-4" />
                    <h3 className="text-lg font-medium mb-2">No active incidents</h3>
                    <p className="text-sm text-muted-foreground">
                      {filters.length > 0 ? 'No incidents match the selected filters ' : 'No incidents '}
                      on non-drained {scope === 'links' ? 'links' : 'devices'} in the selected time range.
                    </p>
                  </div>
                )
              }
              const byStatus = scope === 'links' ? linkIncidentsByStatus : deviceIncidentsByStatus
              const sections: { key: string; title: string; description: string; defaultOpen: boolean; incidents: (LinkIncidentV2 | DeviceIncidentV2)[] }[] = [
                { key: 'ongoing', title: 'Ongoing', description: 'Active incidents', defaultOpen: true, incidents: byStatus.ongoing },
                { key: 'pending_resolution', title: 'Pending Resolution', description: 'Incidents awaiting confirmation of resolution', defaultOpen: true, incidents: byStatus.pendingResolution },
                { key: 'resolved_sustained', title: 'Resolved (Sustained)', description: 'Resolved incidents lasting 30 minutes or longer', defaultOpen: true, incidents: byStatus.resolvedSustained },
                ...(showTransient ? [{ key: 'resolved_recent', title: 'Resolved (Transient)', description: 'Resolved incidents lasting less than 30 minutes', defaultOpen: false, incidents: byStatus.resolvedRecent as (LinkIncidentV2 | DeviceIncidentV2)[] }] : []),
              ]
              return (
                <div className="flex flex-col gap-3">
                  {sections.map(({ key, title, description, defaultOpen, incidents: sectionIncidents }) => (
                    <IncidentSection
                      key={key}
                      title={title}
                      description={description}
                      count={sectionIncidents.length}
                      defaultOpen={defaultOpen}
                    >
                      {sectionIncidents.length === 0 ? null : scope === 'links' ? (
                        <ActiveLinkIncidentsTable
                          incidents={sectionIncidents as LinkIncidentV2[]}
                          sortField={sortField}
                          sortDir={sortDir}
                          toggleSort={toggleSort}
                        />
                      ) : (
                        <ActiveDeviceIncidentsTable
                          incidents={sectionIncidents as DeviceIncidentV2[]}
                          sortField={sortField}
                          sortDir={sortDir}
                          toggleSort={toggleSort}
                        />
                      )}
                    </IncidentSection>
                  ))}
                </div>
              )
            })()}
          </>
        )}

        {/* Drained view */}
        {view === 'drained' && (
          <>
            {(() => {
              const isEmpty = scope === 'links' ? drainedLinks.length === 0 : drainedDevices.length === 0
              const entity = scope === 'links' ? 'links' : 'devices'
              if (isEmpty) {
                return (
                  <div className="flex flex-col items-center justify-center py-12 text-center border border-border rounded-lg">
                    <ShieldAlert className="h-12 w-12 text-muted-foreground mb-4" />
                    <h3 className="text-lg font-medium mb-2">No drained {entity}</h3>
                    <p className="text-sm text-muted-foreground">
                      {hasTypeFilter ? `No drained ${entity} have issues of the selected type(s).` : `No ${entity} are currently drained.`}
                    </p>
                  </div>
                )
              }
              return scope === 'links' ? (
                <DrainedLinksTable drainedLinks={drainedLinks} />
              ) : (
                <DrainedDevicesTable drainedDevices={drainedDevices} />
              )
            })()}
          </>
        )}
        </>)}
      </div>
    </div>
  )
}

function ActiveLinkIncidentsTable({
  incidents,
  sortField,
  sortDir,
  toggleSort,
}: {
  incidents: LinkIncidentV2[]
  sortField: string
  sortDir: string
  toggleSort: (field: 'started_at' | 'ended_at' | 'duration') => void
}) {
  // Stable timestamp for computing ongoing durations
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const renderTimestamp = useMemo(() => Date.now(), [incidents])

  const sortIcon = (field: string) => sortField === field ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''

  return (
    <div className="overflow-hidden">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="text-left px-4 py-3 font-medium">Link</th>
            <th className="text-left px-4 py-3 font-medium">Severity</th>
            <th className="text-left px-4 py-3 font-medium">Symptoms</th>
            <th className="text-left px-4 py-3 font-medium cursor-pointer hover:text-foreground" onClick={() => toggleSort('started_at')}>
              Started{sortIcon('started_at')}
            </th>
            <th className="text-left px-4 py-3 font-medium cursor-pointer hover:text-foreground" onClick={() => toggleSort('ended_at')}>
              Ended{sortIcon('ended_at')}
            </th>
            <th className="text-left px-4 py-3 font-medium cursor-pointer hover:text-foreground" onClick={() => toggleSort('duration')}>
              Duration{sortIcon('duration')}
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {incidents.map((inc) => {
            const isOngoing = inc.status === 'ongoing' || inc.status === 'pending_resolution'
            return (
              <tr key={inc.incident_id} className="hover:bg-muted/30">
                <td className="px-4 py-3">
                  <div className="flex items-center gap-1.5">
                    <Link
                      to={`/incidents/links/${encodeURIComponent(inc.incident_id)}`}
                      className="text-primary hover:underline"
                    >
                      {inc.link_code}
                    </Link>
                    <Link
                      to={`/dz/links/${encodeURIComponent(inc.link_pk)}`}
                      state={{ backLabel: 'incidents' }}
                      className="text-muted-foreground hover:text-foreground transition-colors"
                      title="View link detail"
                    >
                      <ExternalLink className="h-3 w-3" />
                    </Link>
                  </div>
                  <div className="text-xs text-muted-foreground/60">
                    {inc.contributor_code} · {inc.link_type}
                    <span className="mx-1">·</span>
                    <span className="font-mono">{inc.side_a_metro} ↔ {inc.side_z_metro}</span>
                  </div>
                </td>
                <td className="px-4 py-3">
                  <SeverityBadge severity={inc.severity} />
                </td>
                <td className="px-4 py-3">
                  <div className="flex items-center gap-1.5 flex-wrap">
                    {inc.symptoms.map((symptom) => (
                      <span key={symptom} className="inline-flex items-center gap-1">
                        <IncidentTypeBadge type={symptom} />
                        {inc.peak_values[symptom] != null && (
                          <span className="text-xs text-muted-foreground">
                            ({symptom === 'packet_loss'
                              ? `${inc.peak_values[symptom].toFixed(0)}%`
                              : inc.peak_values[symptom]})
                          </span>
                        )}
                      </span>
                    ))}
                    {inc.is_drained && <DrainedBadge />}
                  </div>
                </td>
                <td className="px-4 py-3">
                  <div>{formatTimeAgo(inc.started_at)}</div>
                  <div className="text-[11px] text-muted-foreground/50">{formatTimestamp(inc.started_at)}</div>
                </td>
                <td className="px-4 py-3">
                  {isOngoing
                    ? <span className="text-muted-foreground">—</span>
                    : <>
                        <div>{formatTimeAgo(inc.ended_at!)}</div>
                        <div className="text-[11px] text-muted-foreground/50">{formatTimestamp(inc.ended_at!)}</div>
                      </>
                  }
                </td>
                <td className="px-4 py-3">
                  {isOngoing
                    ? formatDuration(Math.floor((renderTimestamp - new Date(inc.started_at).getTime()) / 1000))
                    : formatDuration(inc.duration_seconds)
                  }
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function ActiveDeviceIncidentsTable({
  incidents,
  sortField,
  sortDir,
  toggleSort,
}: {
  incidents: DeviceIncidentV2[]
  sortField: string
  sortDir: string
  toggleSort: (field: 'started_at' | 'ended_at' | 'duration') => void
}) {
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const renderTimestamp = useMemo(() => Date.now(), [incidents])

  const sortIcon = (field: string) => sortField === field ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''

  return (
    <div className="overflow-hidden">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="text-left px-4 py-3 font-medium">Device</th>
            <th className="text-left px-4 py-3 font-medium">Severity</th>
            <th className="text-left px-4 py-3 font-medium">Symptoms</th>
            <th className="text-left px-4 py-3 font-medium cursor-pointer hover:text-foreground" onClick={() => toggleSort('started_at')}>
              Started{sortIcon('started_at')}
            </th>
            <th className="text-left px-4 py-3 font-medium cursor-pointer hover:text-foreground" onClick={() => toggleSort('ended_at')}>
              Ended{sortIcon('ended_at')}
            </th>
            <th className="text-left px-4 py-3 font-medium cursor-pointer hover:text-foreground" onClick={() => toggleSort('duration')}>
              Duration{sortIcon('duration')}
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {incidents.map((inc) => {
            const isOngoing = inc.status === 'ongoing' || inc.status === 'pending_resolution'
            return (
              <tr key={inc.incident_id} className="hover:bg-muted/30">
                <td className="px-4 py-3">
                  <div className="flex items-center gap-1.5">
                    <Link
                      to={`/incidents/devices/${encodeURIComponent(inc.incident_id)}`}
                      className="text-primary hover:underline"
                    >
                      {inc.device_code}
                    </Link>
                    <Link
                      to={`/dz/devices/${encodeURIComponent(inc.device_pk)}`}
                      className="text-muted-foreground hover:text-foreground transition-colors"
                      title="View device detail"
                    >
                      <ExternalLink className="h-3 w-3" />
                    </Link>
                  </div>
                  <div className="text-xs text-muted-foreground/60">
                    {inc.contributor_code} · {inc.device_type}
                    {inc.metro && <><span className="mx-1">·</span><span className="font-mono">{inc.metro}</span></>}
                  </div>
                </td>
                <td className="px-4 py-3">
                  <SeverityBadge severity={inc.severity} />
                </td>
                <td className="px-4 py-3">
                  <div className="flex items-center gap-1.5 flex-wrap">
                    {inc.symptoms.map((symptom) => (
                      <span key={symptom} className="inline-flex items-center gap-1">
                        <IncidentTypeBadge type={symptom} />
                        {inc.peak_values[symptom] != null && (
                          <span className="text-xs text-muted-foreground">
                            ({inc.peak_values[symptom]})
                          </span>
                        )}
                      </span>
                    ))}
                    {inc.is_drained && <DrainedBadge />}
                  </div>
                </td>
                <td className="px-4 py-3">
                  <div>{formatTimeAgo(inc.started_at)}</div>
                  <div className="text-[11px] text-muted-foreground/50">{formatTimestamp(inc.started_at)}</div>
                </td>
                <td className="px-4 py-3">
                  {isOngoing
                    ? <span className="text-muted-foreground">—</span>
                    : <>
                        <div>{formatTimeAgo(inc.ended_at!)}</div>
                        <div className="text-[11px] text-muted-foreground/50">{formatTimestamp(inc.ended_at!)}</div>
                      </>
                  }
                </td>
                <td className="px-4 py-3">
                  {isOngoing
                    ? formatDuration(Math.floor((renderTimestamp - new Date(inc.started_at).getTime()) / 1000))
                    : formatDuration(inc.duration_seconds)
                  }
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function DrainedLinksTable({ drainedLinks }: { drainedLinks: DrainedLinkInfoV2[] }) {
  return (
    <div className="border border-border rounded-lg overflow-hidden">
      <table className="w-full text-sm table-fixed">
        <thead className="bg-muted/50">
          <tr>
            <th className="text-left px-4 py-3 font-medium w-[24%]">Link</th>
            <th className="text-left px-4 py-3 font-medium w-[10%]">Route</th>
            <th className="text-left px-4 py-3 font-medium w-[11%]">Drain Status</th>
            <th className="text-left px-4 py-3 font-medium w-[16%]">Issues</th>
            <th className="text-left px-4 py-3 font-medium w-[16%]">Started</th>
            <th className="text-left px-4 py-3 font-medium w-[9%] whitespace-nowrap">Clear For</th>
            <th className="text-left px-4 py-3 font-medium w-[14%]">Readiness</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {drainedLinks.map((dl) => (
            <tr key={dl.link_pk} className="hover:bg-muted/30">
              <td className="px-4 py-3 truncate">
                <Link
                  to={`/dz/links/${encodeURIComponent(dl.link_pk)}`}
                  state={{ backLabel: 'incidents' }}
                  className="text-primary hover:underline inline-flex items-center gap-1 max-w-full"
                  title={dl.link_code}
                >
                  <span className="truncate">{dl.link_code}</span>
                  <ExternalLink className="h-3 w-3 shrink-0" />
                </Link>
                <div className="text-xs text-muted-foreground truncate">{dl.contributor_code} · {dl.link_type}</div>
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                <span className="font-mono">
                  {dl.side_a_metro} ↔ {dl.side_z_metro}
                </span>
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-slate-100 text-slate-800 dark:bg-slate-800 dark:text-slate-200">
                  {dl.drain_status}
                </span>
              </td>
              <td className="px-4 py-3">
                {(() => {
                  const allIncidents = [...dl.active_incidents, ...dl.recent_incidents]
                  if (allIncidents.length === 0) {
                    return <span className="text-muted-foreground">-</span>
                  }
                  const symptoms = dedupeSymptoms(allIncidents)
                  return (
                    <div className="flex items-center gap-1.5 flex-wrap">
                      {symptoms.map((symptom) => (
                        <IncidentTypeBadge key={symptom} type={symptom} />
                      ))}
                    </div>
                  )
                })()}
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                {(() => {
                  const allIncidents = [...dl.active_incidents, ...dl.recent_incidents]
                  if (allIncidents.length > 0) {
                    const earliest = allIncidents.reduce((a, b) =>
                      new Date(a.started_at) < new Date(b.started_at) ? a : b
                    )
                    return (
                      <>
                        <div>{formatTimeAgo(earliest.started_at)}</div>
                        <div className="text-xs text-muted-foreground">{formatTimestamp(earliest.started_at)}</div>
                      </>
                    )
                  }
                  if (dl.drained_since) {
                    return (
                      <>
                        <div className="text-muted-foreground">{formatTimeAgo(dl.drained_since)}</div>
                        <div className="text-xs text-muted-foreground">{formatTimestamp(dl.drained_since)}</div>
                      </>
                    )
                  }
                  return <span className="text-muted-foreground">-</span>
                })()}
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                {dl.clear_for_seconds != null ? (
                  formatDuration(dl.clear_for_seconds)
                ) : dl.active_incidents.length > 0 ? (
                  <span className="text-red-600 dark:text-red-400">-</span>
                ) : (
                  <span className="text-muted-foreground">-</span>
                )}
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                <ReadinessDot readiness={dl.readiness} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function DrainedDevicesTable({ drainedDevices }: { drainedDevices: DrainedDeviceInfoV2[] }) {
  return (
    <div className="border border-border rounded-lg overflow-hidden">
      <table className="w-full text-sm table-fixed">
        <thead className="bg-muted/50">
          <tr>
            <th className="text-left px-4 py-3 font-medium w-[24%]">Device</th>
            <th className="text-left px-4 py-3 font-medium w-[10%]">Metro</th>
            <th className="text-left px-4 py-3 font-medium w-[11%]">Drain Status</th>
            <th className="text-left px-4 py-3 font-medium w-[16%]">Issues</th>
            <th className="text-left px-4 py-3 font-medium w-[16%]">Started</th>
            <th className="text-left px-4 py-3 font-medium w-[9%] whitespace-nowrap">Clear For</th>
            <th className="text-left px-4 py-3 font-medium w-[14%]">Readiness</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {drainedDevices.map((dd) => (
            <tr key={dd.device_pk} className="hover:bg-muted/30">
              <td className="px-4 py-3 truncate">
                <Link
                  to={`/dz/devices/${encodeURIComponent(dd.device_pk)}`}
                  className="text-primary hover:underline inline-flex items-center gap-1 max-w-full"
                  title={dd.device_code}
                >
                  <span className="truncate">{dd.device_code}</span>
                  <ExternalLink className="h-3 w-3 shrink-0" />
                </Link>
                <div className="text-xs text-muted-foreground truncate">{dd.contributor_code} · {dd.device_type}</div>
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                <span className="font-mono">{dd.metro}</span>
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-slate-100 text-slate-800 dark:bg-slate-800 dark:text-slate-200">
                  {dd.drain_status}
                </span>
              </td>
              <td className="px-4 py-3">
                {(() => {
                  const allIncidents = [...dd.active_incidents, ...dd.recent_incidents]
                  if (allIncidents.length === 0) {
                    return <span className="text-muted-foreground">-</span>
                  }
                  const symptoms = dedupeSymptoms(allIncidents)
                  return (
                    <div className="flex items-center gap-1.5 flex-wrap">
                      {symptoms.map((symptom) => (
                        <IncidentTypeBadge key={symptom} type={symptom} />
                      ))}
                    </div>
                  )
                })()}
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                {(() => {
                  const allIncidents = [...dd.active_incidents, ...dd.recent_incidents]
                  if (allIncidents.length > 0) {
                    const earliest = allIncidents.reduce((a, b) =>
                      new Date(a.started_at) < new Date(b.started_at) ? a : b
                    )
                    return (
                      <>
                        <div>{formatTimeAgo(earliest.started_at)}</div>
                        <div className="text-xs text-muted-foreground">{formatTimestamp(earliest.started_at)}</div>
                      </>
                    )
                  }
                  if (dd.drained_since) {
                    return (
                      <>
                        <div className="text-muted-foreground">{formatTimeAgo(dd.drained_since)}</div>
                        <div className="text-xs text-muted-foreground">{formatTimestamp(dd.drained_since)}</div>
                      </>
                    )
                  }
                  return <span className="text-muted-foreground">-</span>
                })()}
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                {dd.clear_for_seconds != null ? (
                  formatDuration(dd.clear_for_seconds)
                ) : dd.active_incidents.length > 0 ? (
                  <span className="text-red-600 dark:text-red-400">-</span>
                ) : (
                  <span className="text-muted-foreground">-</span>
                )}
              </td>
              <td className="px-4 py-3 whitespace-nowrap">
                <ReadinessDot readiness={dd.readiness} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
