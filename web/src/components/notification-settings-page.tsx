import { useState, useEffect, useRef, useCallback } from 'react'
import { Link } from 'react-router-dom'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { ArrowLeft, Plus, Trash2, Bell, BellOff, Webhook, MessageSquare, X, Eye, EyeOff, Radio } from 'lucide-react'
import { useAuth } from '@/contexts/AuthContext'
import {
  getNotificationConfigs,
  createNotificationConfig,
  updateNotificationConfig,
  deleteNotificationConfig,
  getSlackInstallations,
  streamNotificationPreview,
  type NotificationConfig,
  type NotificationPreview,
  type SlackInstallation,
} from '@/lib/api'
import { ConfirmDialog } from '@/components/confirm-dialog'

const sourceTypes = [
  {
    value: 'escrow_events',
    label: 'Shred Subscription Activity',
    description: 'Seat activity including funding, allocation, withdrawal, and closure',
    excludeFilterKey: 'exclude_signers',
    excludeLabel: 'Exclude Signers',
    excludeDescription: 'Events from these signer/funder keys will be excluded from notifications.',
    excludePlaceholder: 'Signer public key to exclude',
  },
  {
    value: 'user_activity',
    label: 'User Activity',
    description: 'User connections and disconnections on the network',
    excludeFilterKey: 'exclude_owners',
    excludeLabel: 'Exclude Owners',
    excludeDescription: 'Events from users with these owner public keys will be excluded from notifications.',
    excludePlaceholder: 'Owner public key to exclude',
  },
]

const channelTypes = [
  { value: 'slack', label: 'Slack', icon: MessageSquare },
  { value: 'webhook', label: 'Webhook', icon: Webhook },
]

interface ConfigFormData {
  source_type: string
  channel_type: string
  destination: Record<string, string>
  exclude_keys: string[]
  enabled: boolean
}

const emptyForm: ConfigFormData = {
  source_type: 'escrow_events',
  channel_type: 'slack',
  destination: {},
  exclude_keys: [],
  enabled: true,
}

// Get the filter key name for the current source type.
function getExcludeFilterKey(sourceType: string): string {
  return sourceTypes.find(s => s.value === sourceType)?.excludeFilterKey || 'exclude_keys'
}

// Serialize form exclude_keys to the source-specific filter JSON.
function serializeFilters(sourceType: string, excludeKeys: string[]): Record<string, unknown> {
  if (excludeKeys.length === 0) return {}
  return { [getExcludeFilterKey(sourceType)]: excludeKeys }
}

// Deserialize source-specific filter JSON to a flat exclude_keys list.
function deserializeFilters(sourceType: string, filters: Record<string, unknown> | undefined): string[] {
  if (!filters) return []
  const key = getExcludeFilterKey(sourceType)
  const val = filters[key]
  if (Array.isArray(val)) return val as string[]
  return []
}

export function NotificationSettingsPage() {
  const { user } = useAuth()
  const [configs, setConfigs] = useState<NotificationConfig[]>([])
  const [installations, setInstallations] = useState<SlackInstallation[]>([])
  const [loading, setLoading] = useState(true)
  const [showForm, setShowForm] = useState(false)
  const [editingId, setEditingId] = useState<string | null>(null)
  const [form, setForm] = useState<ConfigFormData>(emptyForm)
  const [signerInput, setSignerInput] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [deleting, setDeleting] = useState<NotificationConfig | null>(null)

  // Preview state
  const [previewSource, setPreviewSource] = useState<string | null>(null)
  const [previewItems, setPreviewItems] = useState<NotificationPreview[]>([])
  const [previewCaughtUp, setPreviewCaughtUp] = useState(false)
  const abortRef = useRef<AbortController | null>(null)
  const previewEndRef = useRef<HTMLDivElement | null>(null)

  const startPreview = useCallback((sourceType: string) => {
    abortRef.current?.abort()

    setPreviewSource(sourceType)
    setPreviewItems([])
    setPreviewCaughtUp(false)

    const controller = new AbortController()
    abortRef.current = controller

    streamNotificationPreview(sourceType, {
      onNotification: (preview) => {
        setPreviewItems(prev => [...prev.slice(-99), preview])
      },
      onCaughtUp: () => setPreviewCaughtUp(true),
      onError: (msg) => setError(msg),
    }, controller.signal)
  }, [])

  const stopPreview = useCallback(() => {
    abortRef.current?.abort()
    abortRef.current = null
    setPreviewSource(null)
  }, [])

  // Cleanup on unmount
  useEffect(() => {
    return () => { abortRef.current?.abort() }
  }, [])

  // Auto-scroll preview
  useEffect(() => {
    previewEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [previewItems])

  useEffect(() => {
    loadData()
  }, [])

  async function loadData() {
    setLoading(true)
    try {
      const [cfgs, installs] = await Promise.all([
        getNotificationConfigs(),
        getSlackInstallations().catch(() => [] as SlackInstallation[]),
      ])
      setConfigs(cfgs)
      setInstallations(installs)
    } catch {
      setError('Failed to load notification settings')
    } finally {
      setLoading(false)
    }
  }

  function openCreateForm() {
    setForm(emptyForm)
    setEditingId(null)
    setSignerInput('')
    setError(null)
    setShowForm(true)
  }

  function openEditForm(cfg: NotificationConfig) {
    setForm({
      source_type: cfg.source_type,
      channel_type: cfg.channel_type,
      destination: cfg.destination,
      exclude_keys: deserializeFilters(cfg.source_type, cfg.filters),
      enabled: cfg.enabled,
    })
    setEditingId(cfg.id)
    setSignerInput('')
    setError(null)
    setShowForm(true)
  }

  function closeForm() {
    setShowForm(false)
    setEditingId(null)
    setError(null)
  }

  function addExcludeKey() {
    const key = signerInput.trim()
    if (key && !form.exclude_keys.includes(key)) {
      setForm(prev => ({
        ...prev,
        exclude_keys: [...prev.exclude_keys, key],
      }))
    }
    setSignerInput('')
  }

  function removeExcludeKey(key: string) {
    setForm(prev => ({
      ...prev,
      exclude_keys: prev.exclude_keys.filter(k => k !== key),
    }))
  }

  async function handleSave() {
    setError(null)
    setSaving(true)
    try {
      const filters = serializeFilters(form.source_type, form.exclude_keys)
      if (editingId) {
        await updateNotificationConfig(editingId, {
          channel_type: form.channel_type,
          destination: form.destination,
          enabled: form.enabled,
          filters,
        })
      } else {
        await createNotificationConfig({
          source_type: form.source_type,
          channel_type: form.channel_type,
          destination: form.destination,
          enabled: form.enabled,
          filters,
        })
      }
      closeForm()
      await loadData()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to save')
    } finally {
      setSaving(false)
    }
  }

  async function handleDelete(id: string) {
    try {
      await deleteNotificationConfig(id)
      setDeleting(null)
      await loadData()
    } catch {
      setError('Failed to delete notification config')
      setDeleting(null)
    }
  }

  async function handleToggle(cfg: NotificationConfig) {
    try {
      await updateNotificationConfig(cfg.id, { enabled: !cfg.enabled })
      await loadData()
    } catch {
      setError('Failed to update notification config')
    }
  }

  const channelLabel = (cfg: NotificationConfig) => {
    if (cfg.channel_type === 'slack') {
      const inst = installations.find(i => i.team_id === cfg.destination.team_id)
      const workspace = inst?.team_name || cfg.destination.team_id || 'Unknown'
      return `Slack: ${workspace} #${cfg.destination.channel_id || '?'}`
    }
    if (cfg.channel_type === 'webhook') {
      const url = cfg.destination.url || ''
      return `Webhook: ${url.length > 40 ? url.slice(0, 40) + '...' : url}`
    }
    return cfg.channel_type
  }

  const sourceLabel = (type: string) => {
    return sourceTypes.find(s => s.value === type)?.label || type
  }

  if (!user) {
    return (
      <div className="flex-1 overflow-auto">
        <div className="max-w-2xl mx-auto px-6 py-8">
          <p className="text-sm text-muted-foreground">Sign in to manage notifications.</p>
        </div>
      </div>
    )
  }

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-2xl mx-auto px-6 py-8">
        <div className="flex items-center gap-3 mb-8">
          <Link
            to="/settings"
            className="p-1.5 rounded-md text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
          >
            <ArrowLeft className="h-4 w-4" />
          </Link>
          <h1 className="text-2xl font-semibold text-foreground">Notifications</h1>
        </div>

        {error && (
          <div className="mb-4 px-4 py-3 rounded-lg text-sm bg-destructive/10 text-destructive border border-destructive/20">
            {error}
          </div>
        )}

        {/* Config list */}
        {!showForm && (
          <>
            {loading ? (
              <div className="text-sm text-muted-foreground">Loading...</div>
            ) : configs.length === 0 ? (
              <div className="bg-card border border-border rounded-lg p-8 text-center">
                <Bell className="h-8 w-8 text-muted-foreground mx-auto mb-3" />
                <p className="text-sm text-muted-foreground mb-4">
                  No notifications configured yet. Set up notifications to get alerted about subscription activity.
                </p>
                <button
                  onClick={openCreateForm}
                  className="inline-flex items-center gap-2 px-4 py-2 rounded-md bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 transition-colors"
                >
                  <Plus className="h-4 w-4" />
                  Add Notification
                </button>
              </div>
            ) : (
              <div className="space-y-3">
                {configs.map(cfg => (
                  <div
                    key={cfg.id}
                    className="bg-card border border-border rounded-lg overflow-hidden"
                  >
                    <div className="px-4 py-3 flex items-center justify-between">
                      <button
                        onClick={() => openEditForm(cfg)}
                        className="flex-1 text-left"
                      >
                        <div className="text-sm font-medium text-foreground">
                          {sourceLabel(cfg.source_type)}
                        </div>
                        <div className="text-xs text-muted-foreground mt-0.5">
                          {channelLabel(cfg)}
                        </div>
                        {(() => {
                          const excluded = deserializeFilters(cfg.source_type, cfg.filters)
                          return excluded.length > 0 ? (
                            <div className="text-xs text-muted-foreground mt-0.5">
                              {excluded.length} excluded key{excluded.length !== 1 ? 's' : ''}
                            </div>
                          ) : null
                        })()}
                      </button>
                      <div className="flex items-center gap-2">
                        <button
                          onClick={() => handleToggle(cfg)}
                          className={`p-2 rounded-md transition-colors ${
                            cfg.enabled
                              ? 'text-primary hover:bg-primary/10'
                              : 'text-muted-foreground hover:bg-muted'
                          }`}
                          title={cfg.enabled ? 'Disable' : 'Enable'}
                        >
                          {cfg.enabled ? <Bell className="h-4 w-4" /> : <BellOff className="h-4 w-4" />}
                        </button>
                        <button
                          onClick={() => setDeleting(cfg)}
                          className="p-2 rounded-md text-muted-foreground hover:text-destructive hover:bg-destructive/10 transition-colors"
                          title="Delete"
                        >
                          <Trash2 className="h-4 w-4" />
                        </button>
                      </div>
                    </div>
                  </div>
                ))}

                <button
                  onClick={openCreateForm}
                  className="inline-flex items-center gap-2 px-4 py-2 rounded-md bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 transition-colors"
                >
                  <Plus className="h-4 w-4" />
                  Add Notification
                </button>
              </div>
            )}
          </>
        )}

        {/* Create/Edit form */}
        {showForm && (
          <div className="bg-card border border-border rounded-lg overflow-hidden">
            <div className="px-4 py-3 border-b border-border flex items-center justify-between">
              <h2 className="text-sm font-medium text-foreground">
                {editingId ? 'Edit Notification' : 'New Notification'}
              </h2>
              <button
                onClick={closeForm}
                className="p-1 text-muted-foreground hover:text-foreground transition-colors"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="p-4 space-y-5">
              {/* Source type */}
              {!editingId && (
                <div>
                  <label className="block text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">
                    Event Source
                  </label>
                  <div className="space-y-1">
                    {sourceTypes.map(s => (
                      <button
                        key={s.value}
                        onClick={() => setForm(prev => ({ ...prev, source_type: s.value, exclude_keys: [] }))}
                        className={`w-full text-left px-3 py-2 rounded-md text-sm transition-colors border ${
                          form.source_type === s.value
                            ? 'border-primary bg-primary/5 text-foreground'
                            : 'border-transparent text-muted-foreground hover:bg-muted/30'
                        }`}
                      >
                        <div className="font-medium">{s.label}</div>
                        <div className={`text-xs ${form.source_type === s.value ? 'text-muted-foreground' : 'text-muted-foreground/70'}`}>{s.description}</div>
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {/* Channel type */}
              <div>
                <label className="block text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">
                  Delivery Channel
                </label>
                <div className="flex gap-2">
                  {channelTypes.map(ch => (
                    <button
                      key={ch.value}
                      onClick={() => setForm(prev => ({ ...prev, channel_type: ch.value, destination: {} }))}
                      className={`flex items-center gap-2 px-3 py-2 rounded-md text-sm transition-colors border ${
                        form.channel_type === ch.value
                          ? 'border-primary bg-primary/5 text-foreground'
                          : 'border-border text-muted-foreground hover:border-muted-foreground'
                      }`}
                    >
                      <ch.icon className="h-4 w-4" />
                      {ch.label}
                    </button>
                  ))}
                </div>
              </div>

              {/* Slack destination */}
              {form.channel_type === 'slack' && (
                <div className="space-y-3">
                  <div>
                    <label className="block text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">
                      Slack Workspace
                    </label>
                    {installations.length === 0 ? (
                      <p className="text-sm text-muted-foreground">
                        No Slack workspaces connected.{' '}
                        <Link to="/settings" className="text-primary hover:underline">
                          Connect one first
                        </Link>.
                      </p>
                    ) : (
                      <div className="space-y-1">
                        {installations.map(inst => (
                          <button
                            key={inst.team_id}
                            onClick={() => setForm(prev => ({
                              ...prev,
                              destination: { ...prev.destination, team_id: inst.team_id },
                            }))}
                            className={`w-full text-left px-3 py-2 rounded-md text-sm transition-colors ${
                              form.destination.team_id === inst.team_id
                                ? 'bg-muted/50 text-foreground'
                                : 'text-muted-foreground hover:bg-muted/30'
                            }`}
                          >
                            {inst.team_name || inst.team_id}
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                  <div>
                    <label className={`block text-xs font-medium uppercase tracking-wide mb-2 ${
                      !form.destination.team_id ? 'text-muted-foreground/50' : 'text-muted-foreground'
                    }`}>
                      Channel ID
                    </label>
                    <input
                      type="text"
                      value={form.destination.channel_id || ''}
                      onChange={e => setForm(prev => ({
                        ...prev,
                        destination: { ...prev.destination, channel_id: e.target.value },
                      }))}
                      disabled={!form.destination.team_id}
                      placeholder="C0123ABC456"
                      className="w-full px-3 py-2 rounded-md border border-border bg-background text-foreground text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary disabled:opacity-50 disabled:cursor-not-allowed"
                    />
                    <p className={`text-xs mt-1 ${!form.destination.team_id ? 'text-muted-foreground/50' : 'text-muted-foreground'}`}>
                      {!form.destination.team_id
                        ? 'Select a workspace first'
                        : 'Right-click a channel in Slack and select "View channel details" to find the Channel ID at the bottom.'}
                    </p>
                  </div>
                </div>
              )}

              {/* Webhook destination */}
              {form.channel_type === 'webhook' && (
                <div>
                  <label className="block text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">
                    Webhook URL
                  </label>
                  <input
                    type="url"
                    value={form.destination.url || ''}
                    onChange={e => setForm(prev => ({
                      ...prev,
                      destination: { ...prev.destination, url: e.target.value },
                    }))}
                    placeholder="https://example.com/webhook"
                    className="w-full px-3 py-2 rounded-md border border-border bg-background text-foreground text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary"
                  />
                </div>
              )}

              {/* Exclude filter */}
              {(() => {
                const sourceConfig = sourceTypes.find(s => s.value === form.source_type)
                if (!sourceConfig) return null
                return (
                  <div>
                    <label className="block text-xs font-medium text-muted-foreground uppercase tracking-wide mb-2">
                      {sourceConfig.excludeLabel}
                    </label>
                    <p className="text-xs text-muted-foreground mb-2">
                      {sourceConfig.excludeDescription}
                    </p>
                    <div className="flex gap-2">
                      <input
                        type="text"
                        value={signerInput}
                        onChange={e => setSignerInput(e.target.value)}
                        onKeyDown={e => { if (e.key === 'Enter') { e.preventDefault(); addExcludeKey() } }}
                        placeholder={sourceConfig.excludePlaceholder}
                        className="flex-1 px-3 py-2 rounded-md border border-border bg-background text-foreground text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary"
                      />
                      <button
                        onClick={addExcludeKey}
                        disabled={!signerInput.trim()}
                        className="px-3 py-2 rounded-md border border-border text-sm text-muted-foreground hover:text-foreground hover:bg-muted transition-colors disabled:opacity-50"
                      >
                        Add
                      </button>
                    </div>
                    {form.exclude_keys.length > 0 && (
                      <div className="mt-2 space-y-1">
                        {form.exclude_keys.map(key => (
                          <div
                            key={key}
                            className="flex items-center justify-between px-3 py-1.5 rounded-md bg-muted/50 text-sm"
                          >
                            <code className="text-xs text-muted-foreground font-mono truncate">
                              {key}
                            </code>
                            <button
                              onClick={() => removeExcludeKey(key)}
                              className="p-0.5 text-muted-foreground hover:text-destructive transition-colors ml-2 shrink-0"
                            >
                              <X className="h-3 w-3" />
                            </button>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )
              })()}

              {/* Save / Cancel */}
              <div className="flex gap-3 justify-end pt-2">
                <button
                  onClick={closeForm}
                  className="px-4 py-2 text-sm text-muted-foreground hover:text-foreground hover:bg-muted rounded-md transition-colors"
                >
                  Cancel
                </button>
                <button
                  onClick={handleSave}
                  disabled={saving}
                  className="px-4 py-2 text-sm font-medium rounded-md bg-primary text-primary-foreground hover:bg-primary/90 transition-colors disabled:opacity-50"
                >
                  {saving ? 'Saving...' : editingId ? 'Save Changes' : 'Create'}
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Preview section */}
        {!showForm && !loading && (
          <section className="mt-8">
            <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wide mb-4">
              Preview
            </h2>
            <div className="bg-card border border-border rounded-lg overflow-hidden">
              <div className="px-4 py-3 flex items-center justify-between border-b border-border">
                <div className="flex items-center gap-3">
                  {previewSource ? (
                    <>
                      <Radio className="h-4 w-4 text-primary animate-pulse" />
                      <span className="text-sm text-foreground">
                        Live: {sourceTypes.find(s => s.value === previewSource)?.label}
                      </span>
                      {previewCaughtUp && (
                        <span className="text-xs text-muted-foreground">Watching for new events...</span>
                      )}
                    </>
                  ) : (
                    <span className="text-sm text-muted-foreground">
                      Preview events from a source in realtime
                    </span>
                  )}
                </div>
                <div className="flex items-center gap-2">
                  {!previewSource ? (
                    sourceTypes.map(s => (
                      <button
                        key={s.value}
                        onClick={() => startPreview(s.value)}
                        className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md border border-border text-xs text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
                      >
                        <Eye className="h-3 w-3" />
                        {s.label}
                      </button>
                    ))
                  ) : (
                    <button
                      onClick={stopPreview}
                      className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-md border border-border text-xs text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
                    >
                      <EyeOff className="h-3 w-3" />
                      Stop
                    </button>
                  )}
                </div>
              </div>

              {previewSource && (
                <div className="max-h-96 overflow-y-auto">
                  {previewItems.length === 0 && previewCaughtUp && (
                    <div className="px-4 py-6 text-center text-sm text-muted-foreground">
                      No recent events. New events will appear here as they happen.
                    </div>
                  )}
                  {previewItems.length === 0 && !previewCaughtUp && (
                    <div className="px-4 py-6 text-center text-sm text-muted-foreground">
                      Loading recent events...
                    </div>
                  )}
                  {previewItems.map((item, idx) => (
                    <div
                      key={idx}
                      className={`px-4 py-3 ${idx !== 0 ? 'border-t border-border' : ''}`}
                    >
                      <div className="prose prose-sm dark:prose-invert max-w-none text-sm [&_p]:my-0.5 [&_strong]:text-foreground [&_code]:text-xs [&_code]:text-muted-foreground [&_hr]:my-2">
                        <ReactMarkdown remarkPlugins={[remarkGfm]}>
                          {item.markdown}
                        </ReactMarkdown>
                      </div>
                    </div>
                  ))}
                  <div ref={previewEndRef} />
                </div>
              )}
            </div>
          </section>
        )}
      </div>

      <ConfirmDialog
        isOpen={deleting !== null}
        title="Delete notification"
        message="This notification config will be permanently deleted. This action cannot be undone."
        confirmLabel="Delete"
        onConfirm={() => deleting && handleDelete(deleting.id)}
        onCancel={() => setDeleting(null)}
      />
    </div>
  )
}
