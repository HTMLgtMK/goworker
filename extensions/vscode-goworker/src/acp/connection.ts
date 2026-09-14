import { Socket } from 'node:net';
import { randomUUID } from 'node:crypto';

interface JSONRPCRequest {
  jsonrpc: '2.0';
  id: string;
  method: string;
  params?: unknown;
}

interface JSONRPCResponse {
  jsonrpc: '2.0';
  id: string;
  result?: unknown;
  error?: { code: number; message: string };
}

interface JSONRPCNotification {
  jsonrpc: '2.0';
  method: string;
  params?: unknown;
}

type JSONRPCMessage = JSONRPCResponse | JSONRPCNotification;

export class ACPConnection {
  private socket?: Socket;
  private connecting?: Promise<void>;
  private connectingSocket?: Socket;
  private cancelConnecting?: () => void;
  private buffer = '';
  private readonly pending = new Map<string, { resolve(value: unknown): void; reject(reason: Error): void }>();
  private readonly listeners = new Set<(method: string, params: unknown) => void>();

  constructor(private readonly socketPath: string) {}

  get connected(): boolean {
    return this.socket?.readyState === 'open';
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    if (this.connecting) return this.connecting;
    if (this.socket) this.close();

    const attempt = new Promise<void>((resolve, reject) => {
      const socket = new Socket();
      this.connectingSocket = socket;
      this.cancelConnecting = () => {
        socket.destroy();
        reject(new Error('Dispatcher connection closed'));
      };
      const fail = (error: Error) => {
        socket.destroy();
        reject(error);
      };
      socket.once('error', fail);
      socket.connect(this.socketPath, () => {
        socket.off('error', fail);
        if (this.connectingSocket !== socket) {
          socket.destroy();
          reject(new Error('Dispatcher connection was superseded'));
          return;
        }
        this.connectingSocket = undefined;
        this.cancelConnecting = undefined;
        this.socket = socket;
        this.bind(socket);
        resolve();
      });
    });
    this.connecting = attempt;

    try {
      await attempt;
    } finally {
      if (this.connecting === attempt) this.connecting = undefined;
    }
  }

  onNotification(listener: (method: string, params: unknown) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  async request<T>(method: string, params?: unknown): Promise<T> {
    if (!this.socket || !this.connected) throw new Error('Dispatcher is not connected');
    const id = randomUUID();
    const request: JSONRPCRequest = { jsonrpc: '2.0', id, method, params };
    const response = new Promise<T>((resolve, reject) => this.pending.set(id, { resolve, reject }));
    this.write(request);
    return response;
  }

  notify(method: string, params?: unknown): void {
    if (!this.socket || !this.connected) return;
    this.write({ jsonrpc: '2.0', method, params } satisfies JSONRPCNotification);
  }

  close(): void {
    const error = new Error('Dispatcher connection closed');
    for (const { reject } of this.pending.values()) reject(error);
    this.pending.clear();
    this.socket?.destroy();
    this.socket = undefined;
    this.cancelConnecting?.();
    this.cancelConnecting = undefined;
    this.connectingSocket = undefined;
    this.connecting = undefined;
    this.buffer = '';
  }

  private bind(socket: Socket): void {
    socket.setEncoding('utf8');
    socket.on('data', (chunk: string) => this.consume(chunk));
    socket.on('error', () => this.closeSocket(socket));
    socket.on('close', () => this.closeSocket(socket));
  }

  private closeSocket(socket: Socket): void {
    if (this.socket === socket) this.close();
  }

  private consume(chunk: string): void {
    this.buffer += chunk;
    for (;;) {
      const newline = this.buffer.indexOf('\n');
      if (newline < 0) return;
      const line = this.buffer.slice(0, newline);
      this.buffer = this.buffer.slice(newline + 1);
      if (line.trim() === '') continue;
      try {
        this.dispatch(JSON.parse(line) as JSONRPCMessage);
      } catch {
        // Dispatcher protocol ignores invalid lines too; never promote transport noise to executable content.
      }
    }
  }

  private dispatch(message: JSONRPCMessage): void {
    if ('id' in message && typeof message.id === 'string') {
      const pending = this.pending.get(message.id);
      if (!pending) return;
      this.pending.delete(message.id);
      if (message.error) pending.reject(new Error(`RPC ${message.error.code}: ${message.error.message}`));
      else pending.resolve(message.result);
      return;
    }
    if ('method' in message && typeof message.method === 'string') {
      for (const listener of this.listeners) listener(message.method, message.params);
    }
  }

  private write(value: JSONRPCRequest | JSONRPCNotification): void {
    this.socket?.write(`${JSON.stringify(value)}\n`);
  }
}
