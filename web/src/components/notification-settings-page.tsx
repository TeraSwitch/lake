import { useState, useEffect, useRef, useCallback } from 'react'
import { Link } from 'react-router-dom'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import remarkBreaks from 'remark-breaks'
import { ArrowLeft, Plus, Trash2, Bell, BellOff, X, Radio, Globe, Pencil } from 'lucide-react'
import { useAuth } from '@/contexts/AuthContext'
import {
  getWebhookEndpoints,
  createWebhookEndpoint,
  updateWebhookEndpoint,
  deleteWebhookEndpoint,
  getNotificationConfigs,
  createNotificationConfig,
  updateNotificationConfig,
  deleteNotificationConfig,
  streamNotificationPreview,
  type WebhookEndpoint,
  type NotificationConfig,
  type NotificationPreview,
} from '@/lib/api'
import { ConfirmDialog } from '@/components/confirm-dialog'

const sourceTypes = [
  {
    value: 'escrow_events',
    label: 'Shred Subscription Activity',
    description: 'Seat activity including funding, allocation, withdrawal, and closure',
    excludeFilterKey: 'exclude_signers',
    excludeLabel: 'Exclude Signers',
    excludeDescription: 'Events from these signer/funder keys will be excluded.',
    excludePlaceholder: 'Signer public key to exclude',
  },
  {
    value: 'user_activity',
    label: 'User Activity',
    description: 'User connections and disconnections on the network',
    excludeFilterKey: 'exclude_owners',
    excludeLabel: 'Exclude Owners',
    excludeDescription: 'Events from users with these owner public keys will be excluded.',
    excludePlaceholder: 'Owner public key to exclude',
  },
]

function getExcludeFilterKey(sourceType: string): string {
  return sourceTypes.find(s => s.value === sourceType)?.excludeFilterKey || 'exclude_keys'
}

function serializeFilters(sourceType: string, excludeKeys: string[]): Record<string, unknown> {
  if (excludeKeys.length === 0) return {}
  return { [getExcludeFilterKey(sourceType)]: excludeKeys }
}

function deserializeFilters(sourceType: string, filters: Record<string, unknown> | undefined): string[] {
  if (!filters) return []
  const key = getExcludeFilterKey(sourceType)
  const val = filters[key]
  if (Array.isArray(val)) return val as string[]
  return []
}

export function NotificationSettingsPage() {
  const { user } = useAuth()
  const [endpoints, setEndpoints] = useState<WebhookEndpoint[]>([])
  const [configs, setConfigs] = useState<NotificationConfig[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Endpoint form
  const [showEndpointForm, setShowEndpointForm] = useState(false)
  const [editingEndpointId, setEditingEndpointId] = useState<string | null>(null)
  const [endpointName, setEndpointName] = useState('')
  const [endpointUrl, setEndpointUrl] = useState('')
  const [endpointFormat, setEndpointFormat] = useState('markdown')
  const [savingEndpoint, setSavingEndpoint] = useState(false)

  // Subscription form
  const [showSubForm, setShowSubForm] = useState(false)
  const [editingSubId, setEditingSubId] = useState<string | null>(null)
  const [subEndpointId, setSubEndpointId] = useState('')
  const [subSourceType, setSubSourceType] = useState('escrow_events')
  const [subExcludeKeys, setSubExcludeKeys] = useState<string[]>([])
  const [excludeInput, setExcludeInput] = useState('')
  const [savingSub, setSavingSub] = useState(false)

  // Delete confirmation
  const [deletingEndpoint, setDeletingEndpoint] = useState<WebhookEndpoint | null>(null)
  const [deletingConfig, setDeletingConfig] = useState<NotificationConfig | null>(null)

  // Preview
  const [previewItems, setPreviewItems] = useState<NotificationPreview[]>([])
  const [previewCaughtUp, setPreviewCaughtUp] = useState(false)
  const [previewActive, setPreviewActive] = useState(false)
  const abortRef = useRef<AbortController | null>(null)
  const previewEndRef = useRef<HTMLDivElement | null>(null)

  const startPreview = useCallback((sourceType: string, filters: Record<string, unknown>) => {
    abortRef.current?.abort()
    setPreviewItems([])
    setPreviewCaughtUp(false)
    setPreviewActive(true)
    const controller = new AbortController()
    abortRef.current = controller
    streamNotificationPreview(sourceType, {
      onNotification: (preview) => setPreviewItems(prev => [...prev.slice(-99), preview]),
      onCaughtUp: () => setPreviewCaughtUp(true),
      onError: (msg) => { if (!controller.signal.aborted) setError(msg) },
    }, controller.signal, filters)
  }, [])

  const stopPreview = useCallback(() => {
    abortRef.current?.abort()
    abortRef.current = null
    setPreviewActive(false)
  }, [])

  useEffect(() => { return () => { abortRef.current?.abort() } }, [])

  const wasCaughtUp = useRef(false)
  useEffect(() => {
    if (!previewCaughtUp) { wasCaughtUp.current = false; return }
    previewEndRef.current?.scrollIntoView({ behavior: wasCaughtUp.current ? 'smooth' : 'instant' })
    wasCaughtUp.current = true
  }, [previewItems, previewCaughtUp])

  // Auto-start preview when subscription form is open
  useEffect(() => {
    if (!showSubForm) { stopPreview(); return }
    const filters = serializeFilters(subSourceType, subExcludeKeys)
    startPreview(subSourceType, filters)
    return () => { abortRef.current?.abort() }
  }, [showSubForm, subSourceType, subExcludeKeys, startPreview, stopPreview])

  useEffect(() => { loadData() }, [])

  async function loadData() {
    setLoading(true)
    try {
      const [eps, cfgs] = await Promise.all([getWebhookEndpoints(), getNotificationConfigs()])
      setEndpoints(eps)
      setConfigs(cfgs)
    } catch {
      setError('Failed to load data')
    } finally {
      setLoading(false)
    }
  }

  // --- Endpoint actions ---

  function openCreateEndpoint() {
    setEditingEndpointId(null)
    setEndpointName('')
    setEndpointUrl('')
    setEndpointFormat('markdown')
    setShowEndpointForm(true)
  }

  function openEditEndpoint(ep: WebhookEndpoint) {
    setEditingEndpointId(ep.id)
    setEndpointName(ep.name)
    setEndpointUrl(ep.url)
    setEndpointFormat(ep.output_format || 'markdown')
    setShowEndpointForm(true)
  }

  async function handleSaveEndpoint() {
    setError(null)
    setSavingEndpoint(true)
    try {
      if (editingEndpointId) {
        await updateWebhookEndpoint(editingEndpointId, { name: endpointName, url: endpointUrl, output_format: endpointFormat })
      } else {
        await createWebhookEndpoint({ name: endpointName, url: endpointUrl, output_format: endpointFormat })
      }
      setShowEndpointForm(false)
      await loadData()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to save endpoint')
    } finally {
      setSavingEndpoint(false)
    }
  }

  async function handleDeleteEndpoint(id: string) {
    try {
      await deleteWebhookEndpoint(id)
      setDeletingEndpoint(null)
      await loadData()
    } catch {
      setError('Failed to delete endpoint')
      setDeletingEndpoint(null)
    }
  }

  // --- Subscription actions ---

  function openCreateSub() {
    setEditingSubId(null)
    setSubEndpointId(endpoints[0]?.id || '')
    setSubSourceType('escrow_events')
    setSubExcludeKeys([])
    setExcludeInput('')
    setShowSubForm(true)
  }

  function openEditSub(cfg: NotificationConfig) {
    setEditingSubId(cfg.id)
    setSubEndpointId(cfg.endpoint_id)
    setSubSourceType(cfg.source_type)
    setSubExcludeKeys(deserializeFilters(cfg.source_type, cfg.filters))
    setExcludeInput('')
    setShowSubForm(true)
  }

  async function handleSaveSub() {
    setError(null)
    setSavingSub(true)
    try {
      const filters = serializeFilters(subSourceType, subExcludeKeys)
      if (editingSubId) {
        await updateNotificationConfig(editingSubId, { endpoint_id: subEndpointId, filters })
      } else {
        await createNotificationConfig({ endpoint_id: subEndpointId, source_type: subSourceType, filters })
      }
      setShowSubForm(false)
      await loadData()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to save subscription')
    } finally {
      setSavingSub(false)
    }
  }

  async function handleDeleteConfig(id: string) {
    try {
      await deleteNotificationConfig(id)
      setDeletingConfig(null)
      await loadData()
    } catch {
      setError('Failed to delete subscription')
      setDeletingConfig(null)
    }
  }

  async function handleToggleConfig(cfg: NotificationConfig) {
    try {
      await updateNotificationConfig(cfg.id, { enabled: !cfg.enabled })
      await loadData()
    } catch {
      setError('Failed to update subscription')
    }
  }

  function addExcludeKey() {
    const key = excludeInput.trim()
    if (key && !subExcludeKeys.includes(key)) {
      setSubExcludeKeys(prev => [...prev, key])
    }
    setExcludeInput('')
  }

  const sourceLabel = (type: string) => sourceTypes.find(s => s.value === type)?.label || type
  const endpointLabel = (id: string) => {
    const ep = endpoints.find(e => e.id === id)
    return ep?.name || ep?.url || id
  }

  if (!user) {
    return (
      <div className="flex-1 overflow-auto">
        <div className="max-w-4xl mx-auto px-6 py-8">
          <p className="text-sm text-muted-foreground">Sign in to manage notifications.</p>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-4xl mx-auto px-6 py-8">
        <div className="flex items-center gap-3 mb-8">
          <Link to="/settings" className="p-1.5 rounded-md text-muted-foreground hover:text-foreground hover:bg-muted transition-colors">
            <ArrowLeft className="h-4 w-4" />
          </Link>
          <h1 className="text-2xl font-semibold text-foreground">Notifications</h1>
        </div>

        {error && (
          <div className="mb-4 px-4 py-3 rounded-lg text-sm bg-destructive/10 text-destructive border border-destructive/20">
            {error}
          </div>
        )}

        {/* Webhook Endpoints */}
        <section className="mb-10">
          <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wide mb-4">
            Webhook Endpoints
          </h2>

          {showEndpointForm ? (
            <div className="bg-card border border-border rounded-lg overflow-hidden">
              <div className="px-4 py-3 border-b border-border flex items-center justify-between">
                <h3 className="text-sm font-medium text-foreground">
                  {editingEndpointId ? 'Edit Endpoint' : 'New Endpoint'}
                </h3>
                <button onClick={() => setShowEndpointForm(false)} className="p-1 text-muted-foreground hover:text-foreground transition-colors">
                  <X className="h-4 w-4" />
                </button>
              </div>
              <div className="p-4 space-y-4">
                <div>
                  <label className="block text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">Name</label>
                  <input type="text" value={endpointName} onChange={e => setEndpointName(e.target.value)} placeholder="My webhook"
                    className="w-full px-3 py-2 rounded-md border border-border bg-background text-foreground text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary" />
                </div>
                <div>
                  <label className="block text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">URL</label>
                  <input type="url" value={endpointUrl} onChange={e => setEndpointUrl(e.target.value)} placeholder="https://example.com/webhook"
                    className="w-full px-3 py-2 rounded-md border border-border bg-background text-foreground text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary" />
                </div>
                <div>
                  <label className="block text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">Output Format</label>
                  <div className="flex gap-2">
                    {['markdown', 'plaintext'].map(fmt => (
                      <button key={fmt} onClick={() => setEndpointFormat(fmt)}
                        className={`px-3 py-2 rounded-md text-sm border transition-colors ${endpointFormat === fmt ? 'border-primary bg-primary/5 text-foreground' : 'border-border text-muted-foreground hover:border-muted-foreground'}`}>
                        {fmt}
                      </button>
                    ))}
                  </div>
                </div>
                <div className="flex gap-3 justify-end pt-2">
                  <button onClick={() => setShowEndpointForm(false)} className="px-4 py-2 text-sm text-muted-foreground hover:text-foreground hover:bg-muted rounded-md transition-colors">Cancel</button>
                  <button onClick={handleSaveEndpoint} disabled={savingEndpoint || !endpointUrl}
                    className="px-4 py-2 text-sm font-medium rounded-md bg-primary text-primary-foreground hover:bg-primary/90 transition-colors disabled:opacity-50">
                    {savingEndpoint ? 'Saving...' : editingEndpointId ? 'Save' : 'Create'}
                  </button>
                </div>
              </div>
            </div>
          ) : (
            <>
              {!loading && endpoints.length === 0 ? (
                <div className="bg-card border border-border rounded-lg p-8 text-center">
                  <Globe className="h-8 w-8 text-muted-foreground mx-auto mb-3" />
                  <p className="text-sm text-muted-foreground mb-4">No webhook endpoints configured yet.</p>
                  <button onClick={openCreateEndpoint}
                    className="inline-flex items-center gap-2 px-4 py-2 rounded-md bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 transition-colors">
                    <Plus className="h-4 w-4" /> Add Endpoint
                  </button>
                </div>
              ) : (
                <div className="space-y-2">
                  {endpoints.map(ep => (
                    <div key={ep.id} className="bg-card border border-border rounded-lg px-4 py-3 flex items-center justify-between">
                      <div className="min-w-0">
                        <div className="text-sm font-medium text-foreground">{ep.name || 'Unnamed'}</div>
                        <div className="text-xs text-muted-foreground truncate">{ep.url}</div>
                        <div className="text-xs text-muted-foreground/60">{ep.output_format}</div>
                      </div>
                      <div className="flex items-center gap-1">
                        <button onClick={() => openEditEndpoint(ep)} className="p-2 rounded-md text-muted-foreground hover:text-foreground hover:bg-muted transition-colors" title="Edit">
                          <Pencil className="h-4 w-4" />
                        </button>
                        <button onClick={() => setDeletingEndpoint(ep)} className="p-2 rounded-md text-muted-foreground hover:text-destructive hover:bg-destructive/10 transition-colors" title="Delete">
                          <Trash2 className="h-4 w-4" />
                        </button>
                      </div>
                    </div>
                  ))}
                  <button onClick={openCreateEndpoint}
                    className="inline-flex items-center gap-2 px-4 py-2 rounded-md bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 transition-colors">
                    <Plus className="h-4 w-4" /> Add Endpoint
                  </button>
                </div>
              )}
            </>
          )}
        </section>

        {/* Subscriptions */}
        <section className="mb-10">
          <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wide mb-4">
            Subscriptions
          </h2>

          {showSubForm ? (
            <div className="bg-card border border-border rounded-lg overflow-hidden">
              <div className="px-4 py-3 border-b border-border flex items-center justify-between">
                <h3 className="text-sm font-medium text-foreground">
                  {editingSubId ? 'Edit Subscription' : 'New Subscription'}
                </h3>
                <button onClick={() => setShowSubForm(false)} className="p-1 text-muted-foreground hover:text-foreground transition-colors">
                  <X className="h-4 w-4" />
                </button>
              </div>
              <div className="p-4 space-y-5">
                {/* Endpoint picker */}
                <div>
                  <label className="block text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">Deliver to</label>
                  {endpoints.length === 0 ? (
                    <p className="text-sm text-muted-foreground">No webhook endpoints. Create one above first.</p>
                  ) : (
                    <div className="space-y-1">
                      {endpoints.map(ep => (
                        <button key={ep.id} onClick={() => setSubEndpointId(ep.id)}
                          className={`w-full text-left px-3 py-2 rounded-md text-sm transition-colors border ${subEndpointId === ep.id ? 'border-primary bg-primary/5 text-foreground' : 'border-transparent text-muted-foreground hover:bg-muted/30'}`}>
                          <div className="font-medium">{ep.name || 'Unnamed'}</div>
                          <div className="text-xs text-muted-foreground truncate">{ep.url}</div>
                        </button>
                      ))}
                    </div>
                  )}
                </div>

                {/* Source type */}
                {!editingSubId && (
                  <div>
                    <label className="block text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">Event Source</label>
                    <div className="space-y-1">
                      {sourceTypes.map(s => (
                        <button key={s.value} onClick={() => { setSubSourceType(s.value); setSubExcludeKeys([]) }}
                          className={`w-full text-left px-3 py-2 rounded-md text-sm transition-colors border ${subSourceType === s.value ? 'border-primary bg-primary/5 text-foreground' : 'border-transparent text-muted-foreground hover:bg-muted/30'}`}>
                          <div className="font-medium">{s.label}</div>
                          <div className={`text-xs ${subSourceType === s.value ? 'text-muted-foreground' : 'text-muted-foreground/70'}`}>{s.description}</div>
                        </button>
                      ))}
                    </div>
                  </div>
                )}

                {/* Exclude filter */}
                {(() => {
                  const sourceConfig = sourceTypes.find(s => s.value === subSourceType)
                  if (!sourceConfig) return null
                  return (
                    <div>
                      <label className="block text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">{sourceConfig.excludeLabel}</label>
                      <p className="text-xs text-muted-foreground mb-2">{sourceConfig.excludeDescription}</p>
                      <div className="flex gap-2">
                        <input type="text" value={excludeInput} onChange={e => setExcludeInput(e.target.value)}
                          onKeyDown={e => { if (e.key === 'Enter') { e.preventDefault(); addExcludeKey() } }}
                          placeholder={sourceConfig.excludePlaceholder}
                          className="flex-1 px-3 py-2 rounded-md border border-border bg-background text-foreground text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary" />
                        <button onClick={addExcludeKey} disabled={!excludeInput.trim()}
                          className="px-3 py-2 rounded-md border border-border text-sm text-muted-foreground hover:text-foreground hover:bg-muted transition-colors disabled:opacity-50">Add</button>
                      </div>
                      {subExcludeKeys.length > 0 && (
                        <div className="mt-2 space-y-1">
                          {subExcludeKeys.map(key => (
                            <div key={key} className="flex items-center justify-between px-3 py-1.5 rounded-md bg-muted/50 text-sm">
                              <code className="text-xs text-muted-foreground font-mono truncate">{key}</code>
                              <button onClick={() => setSubExcludeKeys(prev => prev.filter(k => k !== key))}
                                className="p-0.5 text-muted-foreground hover:text-destructive transition-colors ml-2 shrink-0">
                                <X className="h-3 w-3" />
                              </button>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  )
                })()}

                {/* Preview */}
                <div>
                  <div className="flex items-center gap-2 mb-2">
                    <label className="block text-xs font-medium text-muted-foreground uppercase tracking-wide">Preview</label>
                    {previewActive && (
                      <div className="flex items-center gap-1.5">
                        <Radio className="h-3 w-3 text-primary animate-pulse" />
                        <span className="text-xs text-muted-foreground">{previewCaughtUp ? 'Watching for new events...' : 'Loading...'}</span>
                      </div>
                    )}
                  </div>
                  <div className="border border-border rounded-lg overflow-hidden bg-background">
                    <div className="max-h-72 overflow-y-auto">
                      {previewItems.length === 0 && previewCaughtUp && (
                        <div className="px-4 py-6 text-center text-sm text-muted-foreground">No recent events. New events will appear here as they happen.</div>
                      )}
                      {previewItems.length === 0 && !previewCaughtUp && previewActive && (
                        <div className="px-4 py-6 text-center text-sm text-muted-foreground">Loading recent events...</div>
                      )}
                      {previewItems.map((item, idx) => (
                        <div key={idx} className={`px-4 py-3 ${idx !== 0 ? 'border-t border-border' : ''}`}>
                          <div className="prose prose-sm dark:prose-invert max-w-none text-sm [&_p]:my-0.5 [&_strong]:text-foreground [&_code]:text-xs [&_code]:text-muted-foreground [&_hr]:my-2">
                            <ReactMarkdown remarkPlugins={[remarkGfm, remarkBreaks]}>{item.markdown}</ReactMarkdown>
                          </div>
                        </div>
                      ))}
                      <div ref={previewEndRef} />
                    </div>
                  </div>
                </div>

                {/* Save / Cancel */}
                <div className="flex gap-3 justify-end pt-2">
                  <button onClick={() => setShowSubForm(false)} className="px-4 py-2 text-sm text-muted-foreground hover:text-foreground hover:bg-muted rounded-md transition-colors">Cancel</button>
                  <button onClick={handleSaveSub} disabled={savingSub || !subEndpointId}
                    className="px-4 py-2 text-sm font-medium rounded-md bg-primary text-primary-foreground hover:bg-primary/90 transition-colors disabled:opacity-50">
                    {savingSub ? 'Saving...' : editingSubId ? 'Save' : 'Create'}
                  </button>
                </div>
              </div>
            </div>
          ) : (
            <>
              {!loading && configs.length === 0 ? (
                <div className="bg-card border border-border rounded-lg p-8 text-center">
                  <Bell className="h-8 w-8 text-muted-foreground mx-auto mb-3" />
                  <p className="text-sm text-muted-foreground mb-4">No subscriptions configured yet.</p>
                  <button onClick={openCreateSub} disabled={endpoints.length === 0}
                    className="inline-flex items-center gap-2 px-4 py-2 rounded-md bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 transition-colors disabled:opacity-50">
                    <Plus className="h-4 w-4" /> Add Subscription
                  </button>
                  {endpoints.length === 0 && (
                    <p className="text-xs text-muted-foreground mt-2">Create a webhook endpoint first.</p>
                  )}
                </div>
              ) : (
                <div className="space-y-2">
                  {configs.map(cfg => (
                    <div key={cfg.id} className="bg-card border border-border rounded-lg px-4 py-3 flex items-center justify-between">
                      <button onClick={() => openEditSub(cfg)} className="flex-1 text-left min-w-0">
                        <div className="text-sm font-medium text-foreground">{sourceLabel(cfg.source_type)}</div>
                        <div className="text-xs text-muted-foreground mt-0.5">{endpointLabel(cfg.endpoint_id)}</div>
                        {(() => {
                          const excluded = deserializeFilters(cfg.source_type, cfg.filters)
                          return excluded.length > 0 ? (
                            <div className="text-xs text-muted-foreground/60 mt-0.5">{excluded.length} excluded key{excluded.length !== 1 ? 's' : ''}</div>
                          ) : null
                        })()}
                      </button>
                      <div className="flex items-center gap-1">
                        <button onClick={() => handleToggleConfig(cfg)}
                          className={`p-2 rounded-md transition-colors ${cfg.enabled ? 'text-primary hover:bg-primary/10' : 'text-muted-foreground hover:bg-muted'}`}
                          title={cfg.enabled ? 'Disable' : 'Enable'}>
                          {cfg.enabled ? <Bell className="h-4 w-4" /> : <BellOff className="h-4 w-4" />}
                        </button>
                        <button onClick={() => setDeletingConfig(cfg)}
                          className="p-2 rounded-md text-muted-foreground hover:text-destructive hover:bg-destructive/10 transition-colors" title="Delete">
                          <Trash2 className="h-4 w-4" />
                        </button>
                      </div>
                    </div>
                  ))}
                  <button onClick={openCreateSub} disabled={endpoints.length === 0}
                    className="inline-flex items-center gap-2 px-4 py-2 rounded-md bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 transition-colors disabled:opacity-50">
                    <Plus className="h-4 w-4" /> Add Subscription
                  </button>
                </div>
              )}
            </>
          )}
        </section>
      </div>

      <ConfirmDialog isOpen={deletingEndpoint !== null} title="Delete webhook endpoint"
        message={`This will delete the endpoint "${deletingEndpoint?.name || deletingEndpoint?.url}" and all its subscriptions.`}
        confirmLabel="Delete" onConfirm={() => deletingEndpoint && handleDeleteEndpoint(deletingEndpoint.id)} onCancel={() => setDeletingEndpoint(null)} />

      <ConfirmDialog isOpen={deletingConfig !== null} title="Delete subscription"
        message="This subscription will be permanently deleted."
        confirmLabel="Delete" onConfirm={() => deletingConfig && handleDeleteConfig(deletingConfig.id)} onCancel={() => setDeletingConfig(null)} />
    </div>
  )
}
