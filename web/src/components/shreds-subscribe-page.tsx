import { useState, useMemo, useCallback, useEffect, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useWallet } from '@solana/wallet-adapter-react'
import { WalletMultiButton } from '@solana/wallet-adapter-react-ui'
import { PublicKey } from '@solana/web3.js'
import { Link, useSearchParams } from 'react-router-dom'
import {
  Coins,
  Loader2,
  AlertCircle,
  Check,
  ChevronRight,
  ExternalLink,
  AlertTriangle,
  ArrowRight,
  Zap,
} from 'lucide-react'
import {
  fetchShredDevices,
  fetchShredsOverview,
  type ShredDevice,
} from '@/lib/api'
import { ipv4ToU32, isValidIpv4 } from '@/lib/shred-program'
import {
  deriveShredAccounts,
  buildSubscribeInstructions,
} from '@/lib/shred-transactions'
import { useShredAccounts, useUsdcBalance } from '@/hooks/use-shred-accounts'
import { useShredTransaction, type TransactionStatus } from '@/hooks/use-shred-transaction'
import { useEpochInfo } from '@/hooks/use-epoch-info'
import { useDocumentTitle } from '@/hooks/use-document-title'

// ---------------------------------------------------------------------------
// Status indicator component
// ---------------------------------------------------------------------------

function StatusStep({ label, done, active }: { label: string; done: boolean; active: boolean }) {
  return (
    <div className="flex items-center gap-2">
      {done ? (
        <div className="h-5 w-5 rounded-full bg-green-500 flex items-center justify-center">
          <Check className="h-3 w-3 text-white" />
        </div>
      ) : active ? (
        <Loader2 className="h-5 w-5 text-primary animate-spin" />
      ) : (
        <div className="h-5 w-5 rounded-full border-2 border-border" />
      )}
      <span className={`text-sm ${done ? 'text-foreground' : active ? 'text-foreground' : 'text-muted-foreground'}`}>
        {label}
      </span>
    </div>
  )
}

function TransactionProgress({ status, txSignature }: { status: TransactionStatus; txSignature: string | null }) {
  const steps: { key: TransactionStatus[]; label: string }[] = [
    { key: ['signing'], label: 'Signing transaction' },
    { key: ['sending'], label: 'Sending to network' },
    { key: ['confirming'], label: 'Confirming on-chain' },
  ]

  return (
    <div className="flex items-center gap-4">
      {steps.map((step, i) => {
        const done = steps.slice(i + 1).some(s => s.key.some(k => status === k)) || status === 'confirmed'
        const active = step.key.includes(status)
        return (
          <div key={step.label} className="flex items-center gap-2">
            {i > 0 && <ChevronRight className="h-3 w-3 text-muted-foreground" />}
            <StatusStep label={step.label} done={done} active={active} />
          </div>
        )
      })}
      {status === 'confirmed' && txSignature && (
        <>
          <ChevronRight className="h-3 w-3 text-muted-foreground" />
          <a
            href={`https://solscan.io/tx/${txSignature}`}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1 text-sm text-blue-500 hover:underline"
          >
            View on Solscan <ExternalLink className="h-3 w-3" />
          </a>
        </>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Device picker
// ---------------------------------------------------------------------------

function DevicePicker({
  devices,
  selected,
  onSelect,
}: {
  devices: ShredDevice[]
  selected: ShredDevice | null
  onSelect: (d: ShredDevice) => void
}) {
  const [search, setSearch] = useState('')

  const filtered = useMemo(() => {
    if (!search) return devices
    const needle = search.toLowerCase()
    return devices.filter(
      d =>
        d.device_code.toLowerCase().includes(needle) ||
        d.metro_code.toLowerCase().includes(needle),
    )
  }, [devices, search])

  return (
    <div>
      <input
        type="text"
        value={search}
        onChange={e => setSearch(e.target.value)}
        placeholder="Search devices or metros..."
        className="w-full px-3 py-2 text-sm border border-border rounded-lg bg-background mb-3 focus:outline-none focus:ring-2 focus:ring-primary/50"
      />
      <div className="border border-border rounded-lg overflow-hidden max-h-80 overflow-y-auto">
        <table className="w-full">
          <thead className="sticky top-0 bg-card">
            <tr className="text-xs text-left text-muted-foreground border-b border-border">
              <th className="px-4 py-2.5 font-medium">Device</th>
              <th className="px-4 py-2.5 font-medium">Metro</th>
              <th className="px-4 py-2.5 font-medium text-right">Price / Epoch</th>
              <th className="px-4 py-2.5 font-medium text-right">Available Seats</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map(d => {
              const isSelected = selected?.device_key === d.device_key
              const hasSeats = d.available_seats > 0
              return (
                <tr
                  key={d.device_key}
                  onClick={() => hasSeats && onSelect(d)}
                  className={`border-b border-border last:border-b-0 transition-colors ${
                    isSelected
                      ? 'bg-primary/10 border-primary/20'
                      : hasSeats
                        ? 'hover:bg-muted cursor-pointer'
                        : 'opacity-50'
                  }`}
                >
                  <td className="px-4 py-2.5 text-sm font-mono">{d.device_code || d.device_key.slice(0, 8)}</td>
                  <td className="px-4 py-2.5 text-sm">{d.metro_code}</td>
                  <td className="px-4 py-2.5 text-sm tabular-nums text-right">
                    ${d.total_price_dollars}
                  </td>
                  <td className="px-4 py-2.5 text-sm tabular-nums text-right">
                    {d.available_seats > 0 ? (
                      <span>{d.available_seats}</span>
                    ) : (
                      <span className="text-red-500">Full</span>
                    )}
                  </td>
                </tr>
              )
            })}
            {filtered.length === 0 && (
              <tr>
                <td colSpan={4} className="px-4 py-8 text-center text-muted-foreground text-sm">
                  No devices found
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Epoch progress counter
// ---------------------------------------------------------------------------

function formatEta(ms: number): string {
  const totalSeconds = Math.round(ms / 1000)
  const hours = Math.floor(totalSeconds / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60
  if (hours > 0) return `${hours}h ${minutes}m`
  if (minutes > 0) return `${minutes}m ${seconds}s`
  return `${seconds}s`
}

function EpochProgress({
  epoch,
  progressPct,
  remainingMs,
}: {
  epoch: number
  progressPct: number
  remainingMs: number
}) {
  const nearEnd = progressPct >= 90
  return (
    <div className={`flex items-center gap-3 px-3 py-2 rounded-lg border text-sm ${
      nearEnd
        ? 'bg-amber-500/10 border-amber-500/20 text-amber-600 dark:text-amber-400'
        : 'bg-muted/50 border-border text-muted-foreground'
    }`}>
      <span className="font-medium text-foreground shrink-0">Epoch {epoch}</span>
      <div className="flex-1 relative h-1.5 bg-border rounded-full overflow-hidden min-w-0">
        <div
          className={`absolute inset-y-0 left-0 rounded-full ${nearEnd ? 'bg-amber-500' : 'bg-primary'}`}
          style={{ width: `${Math.min(progressPct, 100).toFixed(2)}%` }}
        />
      </div>
      <span className="tabular-nums text-xs shrink-0">{progressPct.toFixed(0)}%</span>
      <span className="text-xs shrink-0">ETA {formatEta(remainingMs)}</span>
      <span className="text-xs text-muted-foreground shrink-0">→ {epoch + 1}</span>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export function ShredsSubscribePage() {
  useDocumentTitle('Subscribe to Shred Delivery')

  const [searchParams] = useSearchParams()
  const deviceParam = searchParams.get('device') || ''
  const { publicKey: wallet, connected } = useWallet()

  // Data fetching
  const { data: pricing, isLoading: pricingLoading, error: pricingError } = useQuery({
    queryKey: ['shred-devices-subscribe'],
    queryFn: () => fetchShredDevices({ limit: 1000, offset: 0, sortBy: 'device', sortDir: 'asc' }),
    select: (data) => data.items,
    refetchInterval: 30_000,
  })

  const { data: epochInfo } = useEpochInfo()

  const { data: overview } = useQuery({
    queryKey: ['shreds-overview'],
    queryFn: fetchShredsOverview,
    refetchInterval: 30_000,
  })

  // Form state
  const [selectedDevice, setSelectedDevice] = useState<ShredDevice | null>(null)
  const configRef = useRef<HTMLElement>(null)

  const handleDeviceSelect = useCallback((device: ShredDevice) => {
    setSelectedDevice(device)
    setTimeout(() => configRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' }), 50)
  }, [])

  // Auto-select device from ?device= query param
  useEffect(() => {
    if (deviceParam && pricing && !selectedDevice) {
      const match = pricing.find(
        d => d.device_code === deviceParam || d.device_key === deviceParam,
      )
      if (match) setSelectedDevice(match)
    }
  }, [deviceParam, pricing, selectedDevice])
  const [clientIp, setClientIp] = useState('')
  const [amountStr, setAmountStr] = useState('')
  const ipValid = clientIp === '' || isValidIpv4(clientIp)
  const amount = parseFloat(amountStr)
  const amountValid = !isNaN(amount) && amount > 0

  // On-chain state
  const devicePubkey = useMemo(() => {
    if (!selectedDevice) return null
    try { return new PublicKey(selectedDevice.device_key) } catch { return null }
  }, [selectedDevice])

  const shredState = useShredAccounts(devicePubkey, clientIp && isValidIpv4(clientIp) ? clientIp : null)
  const { balance: usdcBalance } = useUsdcBalance()

  // Transaction
  const { status: txStatus, txSignature, error: txError, execute, simulate, reset: resetTx } = useShredTransaction()

  // Simulate mode: dev-only, activated via ?simulate=true in the URL
  const simulateMode = import.meta.env.DEV && searchParams.get('simulate') === 'true'

  // Derived calculations
  const pricePerEpoch = selectedDevice ? selectedDevice.total_price_dollars : 0
  const prepaidEpochs = pricePerEpoch > 0 && amountValid ? Math.floor(amount / pricePerEpoch) : 0
  const amountMicro = amountValid ? BigInt(Math.floor(amount * 1_000_000)) : 0n
  const minAmount = pricePerEpoch > 0 ? pricePerEpoch : 0
  const amountBelowMin = amountValid && minAmount > 0 && amount < minAmount
  const insufficientBalance = amountValid && amountMicro > usdcBalance

  // Can submit?
  const canSubmit =
    connected &&
    selectedDevice &&
    isValidIpv4(clientIp) &&
    amountValid &&
    !amountBelowMin &&
    (!insufficientBalance || simulateMode) &&
    txStatus === 'idle'

  const handleSubscribe = useCallback(async () => {
    if (!canSubmit || !wallet || !selectedDevice || !devicePubkey) return

    const clientIpBits = ipv4ToU32(clientIp)

    const accounts = deriveShredAccounts({
      device: devicePubkey,
      metroExchange: new PublicKey(selectedDevice.metro_exchange_key),
      clientIpBits,
      wallet,
    })

    const instructions = buildSubscribeInstructions({
      accounts,
      wallet,
      clientIpBits,
      amountMicro,
      seatExists: shredState.seatExists,
      escrowExists: shredState.escrowExists,
      seatActive: shredState.seatActive,
    })

    await execute(instructions)
  }, [canSubmit, wallet, selectedDevice, devicePubkey, clientIp, amountMicro, shredState, execute])

  const handleSimulate = useCallback(async () => {
    if (!canSubmit || !wallet || !selectedDevice || !devicePubkey) return

    const clientIpBits = ipv4ToU32(clientIp)

    const accounts = deriveShredAccounts({
      device: devicePubkey,
      metroExchange: new PublicKey(selectedDevice.metro_exchange_key),
      clientIpBits,
      wallet,
    })

    const instructions = buildSubscribeInstructions({
      accounts,
      wallet,
      clientIpBits,
      amountMicro,
      seatExists: shredState.seatExists,
      escrowExists: shredState.escrowExists,
      seatActive: shredState.seatActive,
    })

    await simulate(instructions)
  }, [canSubmit, wallet, selectedDevice, devicePubkey, clientIp, amountMicro, shredState, simulate])

  // Loading & error states
  if (pricingLoading) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (pricingError) {
    return (
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <div className="text-lg font-medium mb-2">Unable to load pricing</div>
          <div className="text-sm text-muted-foreground">{pricingError.message}</div>
        </div>
      </div>
    )
  }

  const devices = pricing ?? []

  return (
    <div className="flex-1 overflow-auto">
      <div className="max-w-4xl mx-auto px-4 sm:px-8 py-8">
        {/* Header */}
        <div className="mb-8">
          <div className="flex items-center gap-3 mb-2">
            <Zap className="h-6 w-6 text-primary" />
            <h1 className="text-2xl font-medium">Subscribe to Shred Delivery</h1>
          </div>
          <p className="text-muted-foreground">
            Select a device, configure your subscription, and fund it with USDC to start receiving shreds.
          </p>
        </div>

        <div className="space-y-8">
          {/* Step 1: Select Device */}
          <section>
            <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-3">
              1. Select a Device
            </h2>
            <DevicePicker
              devices={devices}
              selected={selectedDevice}
              onSelect={handleDeviceSelect}
            />
          </section>

          {/* Step 2: Configure */}
          {selectedDevice && (
            <section ref={configRef}>
              <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-3">
                2. Configure Subscription
              </h2>

              <div className="border border-border rounded-lg bg-card p-6 space-y-5">
                {/* Selected device summary */}
                <div className="flex items-center justify-between text-sm">
                  <div>
                    <span className="text-muted-foreground">Device:</span>{' '}
                    <span className="font-mono font-medium">{selectedDevice.device_code}</span>
                    <span className="text-muted-foreground ml-2">({selectedDevice.metro_code})</span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Price:</span>{' '}
                    <span className="font-medium">${selectedDevice.total_price_dollars} / epoch</span>
                  </div>
                </div>

                <div className="border-t border-border" />

                {/* Client IP */}
                <div>
                  <label className="block text-sm font-medium mb-1.5">Client IP Address</label>
                  <input
                    type="text"
                    value={clientIp}
                    onChange={e => { setClientIp(e.target.value); resetTx() }}
                    placeholder="e.g. 192.168.1.100"
                    className={`w-full max-w-xs px-3 py-2 text-sm border rounded-lg bg-background focus:outline-none focus:ring-2 focus:ring-primary/50 font-mono ${
                      clientIp && !ipValid ? 'border-red-500' : 'border-border'
                    }`}
                  />
                  {clientIp && !ipValid && (
                    <p className="text-xs text-red-500 mt-1">Enter a valid IPv4 address</p>
                  )}
                  <p className="text-xs text-muted-foreground mt-1">
                    The IPv4 address of the client that will receive shreds
                  </p>
                </div>

                {/* Amount */}
                <div>
                  <label className="block text-sm font-medium mb-1.5">Amount (USDC)</label>
                  <div className="flex items-center gap-3">
                    <div className="relative max-w-xs">
                      <span className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground text-sm">$</span>
                      <input
                        type="number"
                        value={amountStr}
                        onChange={e => { setAmountStr(e.target.value); resetTx() }}
                        placeholder="0.00"
                        min="0"
                        step="0.01"
                        className={`w-full pl-7 pr-3 py-2 text-sm border rounded-lg bg-background focus:outline-none focus:ring-2 focus:ring-primary/50 tabular-nums ${
                          amountBelowMin ? 'border-red-500' : 'border-border'
                        }`}
                      />
                    </div>
                    {connected && (
                      <span className="text-xs text-muted-foreground">
                        Balance: ${(Number(usdcBalance) / 1e6).toFixed(2)}
                      </span>
                    )}
                  </div>
                  {amountBelowMin && (
                    <p className="text-xs text-red-500 mt-1">
                      Minimum amount is ${minAmount} (1 epoch)
                    </p>
                  )}
                  {insufficientBalance && !amountBelowMin && (
                    <p className="text-xs text-red-500 mt-1">
                      Insufficient USDC balance
                    </p>
                  )}
                  {amountValid && !amountBelowMin && prepaidEpochs > 0 && (
                    <p className="text-xs text-muted-foreground mt-1">
                      Covers ~{prepaidEpochs} epoch{prepaidEpochs !== 1 ? 's' : ''} at ${pricePerEpoch}/epoch
                    </p>
                  )}
                </div>

                {/* On-chain state info */}
                {shredState.seatExists && (
                  <div className="flex items-center gap-2 text-sm px-3 py-2 rounded-lg bg-blue-500/10 border border-blue-500/20 text-blue-600 dark:text-blue-400">
                    <AlertCircle className="h-4 w-4 flex-shrink-0" />
                    <span>
                      A seat already exists for this device + IP.
                      {shredState.seatActive
                        ? ' This will add funds to the existing subscription.'
                        : ' This will re-activate the seat and add funds.'}
                    </span>
                  </div>
                )}

                {/* Epoch progress counter */}
                {epochInfo && (
                  <EpochProgress
                    epoch={epochInfo.epoch}
                    progressPct={epochInfo.progressPct}
                    remainingMs={epochInfo.remainingMs}
                  />
                )}

                {/* Epoch warning */}
                {overview && overview.current_solana_epoch > 0 && !shredState.seatActive && (
                  <EpochWarning currentEpoch={overview.current_solana_epoch} />
                )}
              </div>
            </section>
          )}

          {/* Step 3: Subscribe */}
          {selectedDevice && (
            <section>
              <h2 className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-3">
                3. Subscribe
              </h2>

              {simulateMode && (
                <div className="flex items-center gap-2 text-xs text-amber-600 dark:text-amber-400 mb-3 px-1">
                  <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
                  Simulate mode — transactions will not be submitted
                </div>
              )}

              <div className="border border-border rounded-lg bg-card p-6">
                {!connected ? (
                  <div className="flex flex-col items-center gap-4 py-4">
                    <p className="text-sm text-muted-foreground">Connect your wallet to subscribe</p>
                    <WalletMultiButton />
                  </div>
                ) : (
                  <div className="space-y-4">
                    <div className="flex items-center justify-between">
                      <div className="text-sm">
                        <span className="text-muted-foreground">Connected:</span>{' '}
                        <span className="font-mono text-xs">
                          {wallet?.toBase58().slice(0, 6)}...{wallet?.toBase58().slice(-4)}
                        </span>
                      </div>
                      <WalletMultiButton />
                    </div>

                    {txStatus === 'simulated' ? (
                      <div className="space-y-3">
                        <div className="flex items-center gap-2 text-green-600 dark:text-green-400">
                          <Check className="h-5 w-5" />
                          <span className="font-medium">Simulation passed — transaction is valid. No funds spent.</span>
                        </div>
                        <button
                          onClick={resetTx}
                          className="text-sm text-muted-foreground hover:text-foreground transition-colors"
                        >
                          Try again
                        </button>
                      </div>
                    ) : txStatus === 'confirmed' ? (
                      <div className="space-y-4">
                        <div className="flex items-center gap-2 text-green-600 dark:text-green-400">
                          <Check className="h-5 w-5" />
                          <span className="font-medium">Subscription successful!</span>
                        </div>
                        <TransactionProgress status={txStatus} txSignature={txSignature} />
                        <div className="flex items-center gap-3 pt-2">
                          <Link
                            to="/dz/shreds/subscribers"
                            className="inline-flex items-center gap-1.5 text-sm text-primary hover:underline"
                          >
                            View your seats <ArrowRight className="h-3.5 w-3.5" />
                          </Link>
                          <button
                            onClick={() => {
                              resetTx()
                              setSelectedDevice(null)
                              setClientIp('')
                              setAmountStr('')
                            }}
                            className="text-sm text-muted-foreground hover:text-foreground transition-colors"
                          >
                            Subscribe to another device
                          </button>
                        </div>
                      </div>
                    ) : txStatus === 'error' ? (
                      <div className="space-y-3">
                        <div className="flex items-center gap-2 text-red-500">
                          <AlertCircle className="h-5 w-5" />
                          <span className="text-sm">
                            {simulateMode ? 'Simulation error: ' : ''}{txError}
                          </span>
                        </div>
                        {txSignature && (
                          <TransactionProgress status={txStatus} txSignature={txSignature} />
                        )}
                        <button
                          onClick={resetTx}
                          className="text-sm text-primary hover:underline"
                        >
                          Try again
                        </button>
                      </div>
                    ) : txStatus === 'simulating' ? (
                      <div className="flex items-center gap-2 text-sm text-muted-foreground">
                        <Loader2 className="h-4 w-4 animate-spin" />
                        Simulating on-chain...
                      </div>
                    ) : txStatus !== 'idle' ? (
                      <TransactionProgress status={txStatus} txSignature={txSignature} />
                    ) : simulateMode ? (
                      <button
                        onClick={handleSimulate}
                        disabled={!canSubmit}
                        className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-amber-600 text-white font-medium text-sm hover:bg-amber-500 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                      >
                        <Coins className="h-4 w-4" />
                        {amountValid
                          ? `Simulate — $${amount.toFixed(2)} USDC (no funds sent)`
                          : 'Simulate'}
                      </button>
                    ) : (
                      <button
                        onClick={handleSubscribe}
                        disabled={!canSubmit}
                        className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-primary text-primary-foreground font-medium text-sm hover:bg-primary/90 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                      >
                        <Coins className="h-4 w-4" />
                        {amountValid
                          ? `Subscribe — $${amount.toFixed(2)} USDC`
                          : 'Subscribe'}
                      </button>
                    )}
                  </div>
                )}
              </div>
            </section>
          )}
        </div>
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Epoch warning (stub — we don't have slot-level data from the API yet,
// so we show a generic note for now)
// ---------------------------------------------------------------------------

function EpochWarning({ currentEpoch }: { currentEpoch: number }) {
  // In the CLI, this warns when <10% of the epoch remains.
  // We'd need slot-level data (slot_index, slots_in_epoch) to replicate exactly.
  // For now, show a gentle informational note with the current epoch.
  void currentEpoch
  return (
    <div className="flex items-start gap-2 text-sm px-3 py-2 rounded-lg bg-amber-500/10 border border-amber-500/20 text-amber-600 dark:text-amber-400">
      <AlertTriangle className="h-4 w-4 flex-shrink-0 mt-0.5" />
      <span>
        New subscriptions are activated for the current epoch. If the epoch is almost over,
        your first funded epoch may be shorter than a full epoch.
      </span>
    </div>
  )
}
