declare module 'react-plotly.js' {
  import * as Plotly from 'plotly.js'
  import * as React from 'react'

  interface PlotParams {
    data: Plotly.Data[]
    layout?: Partial<Plotly.Layout>
    config?: Partial<Plotly.Config>
    style?: React.CSSProperties
    className?: string
    useResizeHandler?: boolean
    onInitialized?: (figure: any, graphDiv: any) => void
    onUpdate?: (figure: any, graphDiv: any) => void
    onPurge?: (figure: any, graphDiv: any) => void
    onError?: (err: any) => void
    divId?: string
    revision?: number
    debug?: boolean
    [key: string]: any
  }

  class Plot extends React.Component<PlotParams> {}
  export default Plot
}

declare module 'plotly.js'
