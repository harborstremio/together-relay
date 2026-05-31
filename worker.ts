export interface Env {
  ROOM: DurableObjectNamespace;
}

const ALLOWED_PATH = /^\/r\/([A-Z0-9]{4,8})$/;

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url);

    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response("harbor-together-relay ok", {
        headers: { "content-type": "text/plain", "access-control-allow-origin": "*" },
      });
    }

    const m = url.pathname.match(ALLOWED_PATH);
    if (!m) return new Response("not found", { status: 404 });

    const roomCode = m[1];
    if (req.headers.get("upgrade") !== "websocket") {
      return new Response("expected websocket upgrade", { status: 426 });
    }

    const id = env.ROOM.idFromName(roomCode);
    const stub = env.ROOM.get(id);
    return stub.fetch(req);
  },
};

type Participant = {
  id: string;
  name: string;
  joinedAt: number;
  ready: boolean;
  avatar?: string | null;
  color?: string | null;
};

type SyncState = {
  mediaId: string | null;
  mediaTitle: string | null;
  episode: { season: number; episode: number; name?: string } | null;
  posterUrl: string | null;
  positionSeconds: number;
  playing: boolean;
  updatedAt: number;
  updatedBy: string;
  hostClientId: string | null;
};

type Peer = {
  socket: WebSocket;
  clientId: string;
  name: string;
  joinedAt: number;
  ready: boolean;
  avatar: string | null;
  color: string | null;
  lastStateAt: number;
};

type RoomCommand =
  | { action: "play" }
  | { action: "pause" }
  | { action: "seek"; positionSeconds: number };

const AVATAR_MAX = 600000;

function clampStr(v: unknown, max: number): string | null {
  return typeof v === "string" ? v.slice(0, max) : null;
}

const ROOM_IDLE_MS = 1000 * 60 * 60 * 6;

export class Room {
  private peers = new Map<WebSocket, Peer>();
  private state: DurableObjectState;
  private syncState: SyncState | null = null;
  private hostClientId: string | null = null;
  private started = false;
  private lastActivity = Date.now();

  constructor(state: DurableObjectState) {
    this.state = state;
  }

  async fetch(req: Request): Promise<Response> {
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();

    server.addEventListener("message", (ev) => this.onMessage(server, ev));
    server.addEventListener("close", () => this.onClose(server));
    server.addEventListener("error", () => this.onClose(server));

    return new Response(null, { status: 101, webSocket: client });
  }

  private onMessage(socket: WebSocket, ev: MessageEvent) {
    let msg: any;
    try {
      msg = JSON.parse(typeof ev.data === "string" ? ev.data : new TextDecoder().decode(ev.data as ArrayBuffer));
    } catch {
      return;
    }
    this.lastActivity = Date.now();

    switch (msg.t) {
      case "hello":
        return this.handleHello(socket, msg);
      case "profile":
        return this.handleProfile(socket, msg);
      case "leave":
        return this.handleLeave(socket);
      case "state":
        return this.handleState(socket, msg);
      case "cmd":
        return this.handleCommand(socket, msg);
      case "chat":
        return this.handleChat(socket, msg);
      case "invite":
        return this.handleInvite(socket, msg);
      case "ready":
        return this.handleReady(socket, msg);
      case "host-leaving":
        return this.handleHostLeaving(socket);
      case "claim-host":
        return this.handleClaimHost(socket, msg);
      case "start":
        return this.handleStart(socket);
      case "summon":
        return this.handleSummon(socket, msg);
      case "cursor":
        return this.handleCursor(socket, msg);
      case "draw":
        return this.handleDraw(socket, msg);
      case "presence":
        return this.handlePresence(socket);
      case "ping":
        return this.send(socket, { t: "pong" });
    }
  }

  private handleReady(socket: WebSocket, msg: { ready?: boolean }) {
    const peer = this.peers.get(socket);
    if (!peer) return;
    peer.ready = !!msg.ready;
    this.broadcast({ t: "participant-ready", clientId: peer.clientId, ready: peer.ready });
  }

  private handleClaimHost(socket: WebSocket, msg: { fresh?: boolean }) {
    const peer = this.peers.get(socket);
    if (!peer) return;
    if (this.hostClientId === peer.clientId && !msg.fresh) return;
    this.hostClientId = peer.clientId;
    this.broadcast({ t: "host", hostClientId: this.hostClientId });
    if (msg.fresh) {
      this.started = false;
      this.broadcast({ t: "started", started: false });
      for (const p of this.peers.values()) {
        p.ready = false;
        this.broadcast({ t: "participant-ready", clientId: p.clientId, ready: false });
      }
    }
  }

  private handleStart(socket: WebSocket) {
    const peer = this.peers.get(socket);
    if (!peer || this.hostClientId !== peer.clientId) return;
    this.started = true;
    this.broadcast({ t: "started", started: true });
  }

  private handleSummon(socket: WebSocket, msg: { target?: any }) {
    const peer = this.peers.get(socket);
    if (!peer || !msg.target) return;
    const t = msg.target;
    const okMeta = typeof t.mediaId === "string";
    const okView = typeof t.view === "string";
    if (!okMeta && !okView) return;
    this.broadcast(
      {
        t: "summon",
        from: peer.clientId,
        name: peer.name,
        target: t,
        at: Date.now(),
      },
      socket,
    );
  }

  private handleCursor(socket: WebSocket, msg: { x?: number; y?: number; visible?: boolean; path?: string }) {
    const peer = this.peers.get(socket);
    if (!peer) return;
    if (typeof msg.x !== "number" || typeof msg.y !== "number") return;
    this.broadcast(
      {
        t: "cursor",
        from: peer.clientId,
        name: peer.name,
        x: msg.x,
        y: msg.y,
        visible: !!msg.visible,
        path: typeof msg.path === "string" ? msg.path : "",
      },
      socket,
    );
  }

  private handleDraw(socket: WebSocket, msg: { strokeId?: string; phase?: string; x?: number; y?: number; color?: string; path?: string }) {
    const peer = this.peers.get(socket);
    if (!peer) return;
    if (typeof msg.strokeId !== "string" || msg.strokeId.length === 0 || msg.strokeId.length > 64) return;
    const phase = msg.phase === "start" || msg.phase === "point" || msg.phase === "end" ? msg.phase : null;
    if (!phase) return;
    this.broadcast(
      {
        t: "draw",
        from: peer.clientId,
        name: peer.name,
        strokeId: msg.strokeId,
        phase,
        x: typeof msg.x === "number" ? msg.x : undefined,
        y: typeof msg.y === "number" ? msg.y : undefined,
        color: typeof msg.color === "string" ? msg.color.slice(0, 32) : undefined,
        path: typeof msg.path === "string" ? msg.path : "",
      },
      socket,
    );
  }

  private handlePresence(socket: WebSocket) {
    const peer = this.peers.get(socket);
    if (!peer) return;
    this.broadcast(
      { t: "presence", from: peer.clientId, activeAt: Date.now() },
      socket,
    );
  }

  private handleHostLeaving(socket: WebSocket) {
    const peer = this.peers.get(socket);
    if (!peer) return;
    if (this.hostClientId !== peer.clientId) return;
    this.broadcast(
      { t: "host-leaving", from: peer.clientId, name: peer.name, at: Date.now() },
      socket,
    );
    this.reassignHost(peer.clientId);
  }

  private reassignHost(excludeClientId?: string) {
    let next: Peer | null = null;
    for (const p of this.peers.values()) {
      if (excludeClientId && p.clientId === excludeClientId) continue;
      if (!next || p.joinedAt < next.joinedAt) next = p;
    }
    this.hostClientId = next ? next.clientId : null;
    this.broadcast({ t: "host", hostClientId: this.hostClientId });
  }

  private handleHello(
    socket: WebSocket,
    msg: { clientId?: string; name?: string; avatar?: unknown; color?: unknown },
  ) {
    if (!msg.clientId) {
      this.send(socket, { t: "error", code: "missing_client_id", message: "clientId required" });
      socket.close(1008, "missing_client_id");
      return;
    }
    const name = (msg.name || "Guest").toString().slice(0, 32);
    const peer: Peer = {
      socket,
      clientId: msg.clientId,
      name,
      joinedAt: Date.now(),
      ready: false,
      avatar: clampStr(msg.avatar, AVATAR_MAX),
      color: clampStr(msg.color, 64),
      lastStateAt: 0,
    };

    for (const [s, p] of this.peers) {
      if (p.clientId === msg.clientId && s !== socket) {
        try { s.close(1000, "replaced"); } catch {}
        this.peers.delete(s);
      }
    }

    this.peers.set(socket, peer);
    const becameHost = !this.hostClientId;
    if (becameHost) this.hostClientId = peer.clientId;

    const participants: Participant[] = Array.from(this.peers.values()).map((p) => ({
      id: p.clientId,
      name: p.name,
      joinedAt: p.joinedAt,
      ready: p.ready,
      avatar: p.avatar,
      color: p.color,
    }));

    this.send(socket, {
      t: "joined",
      room: "",
      participants,
      state: this.syncState,
      hostClientId: this.hostClientId,
      started: this.started,
    });

    this.broadcast(
      {
        t: "participant-joined",
        participant: {
          id: peer.clientId,
          name: peer.name,
          joinedAt: peer.joinedAt,
          ready: false,
          avatar: peer.avatar,
          color: peer.color,
        },
      },
      socket,
    );
    if (becameHost) this.broadcast({ t: "host", hostClientId: this.hostClientId }, socket);
  }

  private handleProfile(
    socket: WebSocket,
    msg: { name?: string; avatar?: unknown; color?: unknown },
  ) {
    const peer = this.peers.get(socket);
    if (!peer) return;
    if (typeof msg.name === "string" && msg.name.trim()) peer.name = msg.name.slice(0, 32);
    peer.avatar = clampStr(msg.avatar, AVATAR_MAX);
    peer.color = clampStr(msg.color, 64);
    this.broadcast({
      t: "participant-profile",
      participant: { id: peer.clientId, name: peer.name, avatar: peer.avatar, color: peer.color },
    });
  }

  private handleLeave(socket: WebSocket) {
    const peer = this.peers.get(socket);
    if (!peer) return;
    this.peers.delete(socket);
    this.broadcast({ t: "participant-left", clientId: peer.clientId, name: peer.name });
    if (this.hostClientId === peer.clientId) this.reassignHost();
    try { socket.close(1000, "left"); } catch {}
  }

  private handleState(socket: WebSocket, msg: { state?: SyncState }) {
    const peer = this.peers.get(socket);
    if (!peer || !msg.state) return;
    const incoming = msg.state;
    if (typeof incoming.positionSeconds !== "number" || !isFinite(incoming.positionSeconds) || incoming.positionSeconds < 0) return;
    if (typeof incoming.updatedAt !== "number" || !isFinite(incoming.updatedAt)) return;
    if (typeof incoming.playing !== "boolean") return;
    if (typeof incoming.updatedBy !== "string" || incoming.updatedBy !== peer.clientId) return;
    const isHostWrite = this.hostClientId != null && peer.clientId === this.hostClientId;
    if (this.hostClientId != null && !isHostWrite) return;
    const now = Date.now();
    if (!isHostWrite) {
      if (now - peer.lastStateAt < 500) return;
      if (this.syncState && incoming.updatedAt < this.syncState.updatedAt - 2000) return;
    }
    peer.lastStateAt = now;
    const stamped: SyncState = { ...incoming, hostClientId: this.hostClientId };
    this.syncState = stamped;
    this.broadcast({ t: "state", state: stamped, srvAt: now }, socket);
  }

  private handleCommand(socket: WebSocket, msg: { command?: RoomCommand }) {
    const peer = this.peers.get(socket);
    if (!peer || !msg.command) return;
    const c = msg.command;
    if (c.action !== "play" && c.action !== "pause" && c.action !== "seek") return;
    if (c.action === "seek" && (typeof c.positionSeconds !== "number" || !isFinite(c.positionSeconds) || c.positionSeconds < 0)) return;
    if (!this.hostClientId || peer.clientId === this.hostClientId) return;
    for (const [s, p] of this.peers) {
      if (p.clientId === this.hostClientId) {
        this.send(s, { t: "cmd", from: peer.clientId, command: c });
        return;
      }
    }
  }

  private handleChat(socket: WebSocket, msg: { text?: string }) {
    const peer = this.peers.get(socket);
    if (!peer) return;
    const text = (msg.text || "").toString().trim().slice(0, 500);
    if (!text) return;
    this.broadcast({
      t: "chat",
      from: peer.clientId,
      name: peer.name,
      text,
      at: Date.now(),
    });
  }

  private handleInvite(socket: WebSocket, msg: { invite?: { mediaId?: string } }) {
    const peer = this.peers.get(socket);
    if (!peer || !msg.invite || !msg.invite.mediaId) return;
    this.broadcast(
      {
        t: "invite",
        from: peer.clientId,
        name: peer.name,
        invite: msg.invite,
        at: Date.now(),
      },
      socket,
    );
  }

  private onClose(socket: WebSocket) {
    const peer = this.peers.get(socket);
    if (!peer) return;
    this.peers.delete(socket);
    this.broadcast({ t: "participant-left", clientId: peer.clientId, name: peer.name });
    if (this.hostClientId === peer.clientId) this.reassignHost();
    if (this.peers.size === 0 && Date.now() - this.lastActivity > ROOM_IDLE_MS) {
      this.syncState = null;
      this.hostClientId = null;
      this.started = false;
    }
  }

  private send(socket: WebSocket, msg: unknown) {
    try { socket.send(JSON.stringify(msg)); } catch {}
  }

  private broadcast(msg: unknown, except?: WebSocket) {
    const payload = JSON.stringify(msg);
    for (const [s] of this.peers) {
      if (s === except) continue;
      try { s.send(payload); } catch {}
    }
  }
}
