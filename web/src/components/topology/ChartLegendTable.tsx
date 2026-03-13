import type { UseChartLegendReturn } from '@/hooks/use-chart-legend'
import type { ChartLegendSeries } from './ChartLegend'

interface ChartLegendTableProps {
  series: ChartLegendSeries[]
  legend: UseChartLegendReturn
  /** Map from series key to display value (formatted string) */
  values?: Map<string, string>
  /** Map from series key to max value (formatted string) */
  maxValues?: Map<string, string>
  /** Formatted timestamp to show in the header (hovered or latest) */
  hoveredTime?: string
}

export function ChartLegendTable({ series, legend, values, maxValues, hoveredTime }: ChartLegendTableProps) {
  return (
    <div className="flex flex-col text-xs px-2 pt-1 pb-2">
      <div className="flex items-center px-1 mb-1">
        <span className="text-xs text-muted-foreground flex-1 min-w-0">Name</span>
        {maxValues && <span className="text-xs text-muted-foreground w-28 text-right">Max</span>}
        <span className="text-xs text-muted-foreground w-28 text-right">{hoveredTime ?? 'Value'}</span>
      </div>
      <div className="space-y-0.5">
        {series.map((s) => {
          const opacity = legend.getOpacity(s.key)
          const value = values?.get(s.key)
          const maxValue = maxValues?.get(s.key)
          return (
            <div
              key={s.key}
              className="flex items-center px-1 py-0.5 rounded cursor-pointer hover:bg-muted/50 transition-colors"
              style={{ opacity }}
              onClick={(e) => legend.handleClick(s.key, e)}
              onMouseEnter={() => legend.handleMouseEnter(s.key)}
              onMouseLeave={legend.handleMouseLeave}
            >
              <div className="flex items-center gap-1.5 min-w-0 flex-1">
                {s.dashed ? (
                  <span
                    className="w-3 h-0.5 flex-shrink-0"
                    style={{
                      backgroundImage: `repeating-linear-gradient(90deg, ${s.color} 0, ${s.color} 2px, transparent 2px, transparent 4px)`,
                      backgroundSize: '4px 1px',
                    }}
                  />
                ) : (
                  <span
                    className="w-2.5 h-2.5 rounded-sm flex-shrink-0"
                    style={{ backgroundColor: s.color }}
                  />
                )}
                <span className="font-mono text-foreground truncate">{s.label}</span>
              </div>
              {maxValues && (
                <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-28 text-right">
                  {maxValue ?? '—'}
                </span>
              )}
              <span className="text-muted-foreground font-mono tabular-nums whitespace-nowrap w-28 text-right">
                {value ?? '—'}
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}
