/**
 * powerGridEngine.ts
 * Core telemetry engine for workspace power grid health, socket voltage/wattage delivery,
 * and crowdsourced dead-plug reliability scoring.
 */

export type PowerSocketType =
  | "USB_C_140W_PD"
  | "USB_C_100W_PD"
  | "USB_C_65W"
  | "AC_UNIVERSAL_WALL"
  | "WIRELESS_MAGSAFE";

export type OutletHealthStatus =
  | "OPERATIONAL_OPTIMAL"
  | "THROTTLED_LOW_WATTAGE"
  | "DEAD_NO_POWER"
  | "LOOSE_PHYSICAL_FAULT";

export type PowerGridStatus =
  | "ONLINE_MAINS"
  | "DEGRADED_VOLTAGE"
  | "GENERATOR_ACTIVE"
  | "OUTAGE";

export interface DeskPowerNode {
  seatId: string;
  seatNumber: string;
  socketType: PowerSocketType;
  maxWattageW: number;
  measuredVoltageV: number;
  status: OutletHealthStatus;
  reliabilityScorePct: number; // 0 to 100
  lastVerifiedAt: string;
  reportedIssueCount: number;
  outletLocation: "UNDER_DESK" | "DESK_GROMMET" | "WALL_MOUNTED" | "FLOOR_BOX";
}

export interface VenuePowerGridSummary {
  venueId: string;
  venueName: string;
  gridStatus: PowerGridStatus;
  generatorActive: boolean;
  batteryBackupMinutesRemaining?: number;
  overallGridHealthScore: number; // 0 to 100
  totalSocketsCount: number;
  workingSocketsCount: number;
  fastChargingCoveragePct: number; // % of desks with >= 65W PD
  nominalVoltageV: number; // 120V (US) or 230V (EU/UK)
  nodes: DeskPowerNode[];
  lastGridSweepAt: string;
}

/**
 * Computes overall power grid health score, grid status, and fast charging coverage percentage.
 */
export function computeVenuePowerGridSummary(
  venueId: string,
  venueName: string,
  nodes: DeskPowerNode[],
  nominalVoltage = 120,
  forcedStatus?: PowerGridStatus
): VenuePowerGridSummary {
  const total = nodes.length || 1;
  const working = nodes.filter((n) => n.status === "OPERATIONAL_OPTIMAL").length;
  const fastChargingCount = nodes.filter(
    (n) => n.status === "OPERATIONAL_OPTIMAL" && n.maxWattageW >= 65
  ).length;

  const reliabilitySum = nodes.reduce((sum, n) => sum + n.reliabilityScorePct, 0);
  const overallGridHealthScore = Math.round(reliabilitySum / total);
  const fastChargingCoveragePct = Math.round((fastChargingCount / total) * 100);

  let gridStatus: PowerGridStatus = forcedStatus || "ONLINE_MAINS";
  if (!forcedStatus) {
    if (working === 0 && nodes.length > 0) {
      gridStatus = "OUTAGE";
    } else if (nodes.some((n) => n.status === "THROTTLED_LOW_WATTAGE") && working < total / 2) {
      gridStatus = "DEGRADED_VOLTAGE";
    }
  }

  const generatorActive = gridStatus === "GENERATOR_ACTIVE";
  const batteryBackupMinutesRemaining =
    gridStatus === "OUTAGE" ? 45 : gridStatus === "GENERATOR_ACTIVE" ? 180 : undefined;

  return {
    venueId,
    venueName,
    gridStatus,
    generatorActive,
    batteryBackupMinutesRemaining,
    overallGridHealthScore,
    totalSocketsCount: nodes.length,
    workingSocketsCount: working,
    fastChargingCoveragePct,
    nominalVoltageV: nominalVoltage,
    nodes,
    lastGridSweepAt: new Date().toISOString(),
  };
}

export type PowerGridTelemetryHandler = (update: Partial<VenuePowerGridSummary>) => void;

/**
 * Event-driven telemetry stream subscriber and manager for workspace power grid.
 * Connects to WebSocket telemetry streams or event emitters.
 * Ensures all event listeners, timers, and sockets are cleanly deregistered on unmount.
 */
class PowerGridTelemetryStreamManager {
  private listeners: Map<string, Set<PowerGridTelemetryHandler>> = new Map();
  private timers: Map<string, ReturnType<typeof setInterval>> = new Map();
  private sockets: Map<string, WebSocket> = new Map();

  /**
   * Subscribe a listener callback to real-time power grid telemetry updates for a venue.
   * Returns an unsubscribe teardown function.
   */
  subscribe(
    venueId: string,
    handler: PowerGridTelemetryHandler,
    options?: { pollIntervalMs?: number; wsUrl?: string }
  ): () => void {
    if (!this.listeners.has(venueId)) {
      this.listeners.set(venueId, new Set());
    }
    const handlers = this.listeners.get(venueId)!;
    handlers.add(handler);

    // If WebSocket URL is provided and in browser environment
    if (options?.wsUrl && typeof window !== "undefined" && typeof WebSocket !== "undefined") {
      try {
        if (!this.sockets.has(venueId)) {
          const ws = new WebSocket(options.wsUrl);
          ws.onmessage = (event) => {
            try {
              const data = JSON.parse(event.data);
              this.broadcast(venueId, data);
            } catch {
              // Ignore malformed payloads
            }
          };
          this.sockets.set(venueId, ws);
        }
      } catch {
        // Fall back to event emission
      }
    }

    // Attach custom event listener on window for cross-component / mock telemetry events
    const eventName = `worksphere:power-telemetry:${venueId}`;
    const domEventListener = (event: Event) => {
      const customEvent = event as CustomEvent<Partial<VenuePowerGridSummary>>;
      if (customEvent.detail) {
        handler(customEvent.detail);
      }
    };

    if (typeof window !== "undefined" && typeof window.addEventListener === "function") {
      window.addEventListener(eventName, domEventListener);
    }

    // Return cleanup function to deregister handler and cleanup resources
    return () => {
      this.unsubscribe(venueId, handler);
      if (typeof window !== "undefined" && typeof window.removeEventListener === "function") {
        window.removeEventListener(eventName, domEventListener);
      }
    };
  }

  /**
   * Deregisters a handler and cleans up sockets/timers if no listeners remain.
   */
  unsubscribe(venueId: string, handler: PowerGridTelemetryHandler): void {
    const handlers = this.listeners.get(venueId);
    if (handlers) {
      handlers.delete(handler);
      if (handlers.size === 0) {
        this.listeners.delete(venueId);

        // Clean up socket
        const ws = this.sockets.get(venueId);
        if (ws) {
          try {
            ws.close();
          } catch {
            // Ignore close errors
          }
          this.sockets.delete(venueId);
        }

        // Clean up timer
        const timer = this.timers.get(venueId);
        if (timer) {
          clearInterval(timer);
          this.timers.delete(venueId);
        }
      }
    }
  }

  /**
   * Broadcast an update to all active listeners for a venue.
   */
  broadcast(venueId: string, update: Partial<VenuePowerGridSummary>): void {
    const handlers = this.listeners.get(venueId);
    if (handlers) {
      handlers.forEach((h) => {
        try {
          h(update);
        } catch (e) {
          console.error("Telemetry handler error:", e);
        }
      });
    }
  }

  /**
   * Dispatches a telemetry update event via window CustomEvent and internal broadcast.
   */
  dispatchTelemetryEvent(venueId: string, update: Partial<VenuePowerGridSummary>): void {
    if (typeof window !== "undefined" && typeof window.dispatchEvent === "function") {
      const eventName = `worksphere:power-telemetry:${venueId}`;
      window.dispatchEvent(new CustomEvent(eventName, { detail: update }));
    }
    this.broadcast(venueId, update);
  }

  /**
   * Returns count of active listeners for a venue (useful for memory leak verification).
   */
  getActiveListenersCount(venueId?: string): number {
    if (venueId) {
      return this.listeners.get(venueId)?.size ?? 0;
    }
    let total = 0;
    this.listeners.forEach((set) => {
      total += set.size;
    });
    return total;
  }

  /**
   * Clears all listeners, sockets, and timers across all venues.
   */
  reset(): void {
    this.sockets.forEach((ws) => {
      try {
        ws.close();
      } catch {
        // ignore
      }
    });
    this.sockets.clear();
    this.timers.forEach((t) => clearInterval(t));
    this.timers.clear();
    this.listeners.clear();
  }
}

export const powerGridTelemetryStream = new PowerGridTelemetryStreamManager();

/**
 * Subscribes to real-time power grid telemetry stream. Returns cleanup unsubscribe function.
 */
export function subscribePowerGridTelemetry(
  venueId: string,
  handler: PowerGridTelemetryHandler,
  options?: { pollIntervalMs?: number; wsUrl?: string }
): () => void {
  return powerGridTelemetryStream.subscribe(venueId, handler, options);
}

/**
 * Broadcasts a telemetry update for a venue.
 */
export function broadcastPowerGridTelemetry(
  venueId: string,
  update: Partial<VenuePowerGridSummary>
): void {
  powerGridTelemetryStream.dispatchTelemetryEvent(venueId, update);
}

/**
 * Returns current count of registered telemetry listeners.
 */
export function getActivePowerGridListenersCount(venueId?: string): number {
  return powerGridTelemetryStream.getActiveListenersCount(venueId);
}

