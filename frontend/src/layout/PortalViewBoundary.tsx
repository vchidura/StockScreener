import { Component, type ErrorInfo, type ReactNode } from 'react'
import { AlertTriangle, RotateCcw } from 'lucide-react'
import { useLocation } from 'react-router-dom'

type BoundaryProps = { children: ReactNode; resetKey: string }

class PortalRenderBoundary extends Component<BoundaryProps, { failed: boolean }> {
  state = { failed: false }

  static getDerivedStateFromError() {
    return { failed: true }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('Portal view render failed', error, info)
  }

  componentDidUpdate(previous: BoundaryProps, previousState: { failed: boolean }) {
    if (this.state.failed && previousState.failed && previous.resetKey !== this.props.resetKey) {
      this.setState({ failed: false })
    }
  }

  render() {
    if (!this.state.failed) return this.props.children
    return (
      <section className="tm-view-error" role="alert">
        <AlertTriangle size={20} aria-hidden="true" />
        <div>
          <strong>This page could not be displayed.</strong>
          <p>The rest of the portal is still available. Retry this view or choose another page from navigation.</p>
        </div>
        <button type="button" onClick={() => this.setState({ failed: false })}>
          <RotateCcw size={14} aria-hidden="true" />
          Retry
        </button>
      </section>
    )
  }
}

export function PortalViewBoundary({ children }: { children: ReactNode }) {
  const location = useLocation()
  const resetKey = `${location.pathname}${location.search}`
  return (
    <PortalRenderBoundary key={location.pathname.startsWith('/ticker/') ? location.pathname : resetKey} resetKey={resetKey}>
      {children}
    </PortalRenderBoundary>
  )
}