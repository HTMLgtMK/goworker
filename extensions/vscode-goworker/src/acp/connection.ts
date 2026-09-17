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

// 服务端发来的请求：同时带 id 与 method（区别于通知，也区别于响应）。
interface JSONRPCIncomingRequest {
  jsonrpc: '2.0';
  id: string;
  method: string;
  params?: unknown;
}

interface JSONRPCResult {
  jsonrpc: '2.0';
  id: string;
  result: unknown;
}

interface JSONRPCFailure {
  jsonrpc: '2.0';
  id: string;
  error: { code: number; message: string };
}

type JSONRPCMessage = JSONRPCResponse | JSONRPCNotification | JSONRPCIncomingRequest;

export class ACPConnection {
  private socket?: Socket;
  private connecting?: Promise<void>;
  private connectingSocket?: Socket;
  private cancelConnecting?: () => void;
  private buffer = '';
  private readonly pending = new Map<string, { resolve(value: unknown): void; reject(reason: Error): void }>();
  private readonly listeners = new Set<(method: string, params: unknown) => void>();
  private readonly requestHandlers = new Map<string, (params: unknown) => Promise<unknown>>();

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

  /**
   * 注册服务端→客户端的请求处理器（如 session/request_permission）。
   * 返回值即回给对端的 result；抛错则回成 RPC error。
   *
   * 未注册的请求方法会被显式回以 -32601（method not found），不是静默丢弃：
   * 对端正阻塞等这个应答，丢掉会让它一直挂着。
   */
  onRequest(method: string, handler: (params: unknown) => Promise<unknown>): () => void {
    this.requestHandlers.set(method, handler);
    return () => this.requestHandlers.delete(method);
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
    // 服务端发来的请求：带 id 且带 method。必须与"响应"分开判断——旧代码只看
    // 有没有 id，于是把这类请求当响应处理、在 pending 表里查不到就丢弃，对端
    // 于是永远等不到应答（daemon 侧的 permission 请求就是这样挂住的）。
    if (isIncomingRequest(message)) {
      void this.handleIncomingRequest(message);
      return;
    }
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

  private async handleIncomingRequest(request: JSONRPCIncomingRequest): Promise<void> {
    const handler = this.requestHandlers.get(request.method);
    if (!handler) {
      // 显式回 -32601：对端正阻塞等应答，静默丢弃会让它一直挂着。
      // daemon 侧把 -32601 解释为"该客户端没有这个能力"，据此换下一个订阅者。
      this.write({
        jsonrpc: '2.0',
        id: request.id,
        error: { code: -32601, message: `method not found: ${request.method}` },
      } satisfies JSONRPCFailure);
      return;
    }
    try {
      const result = await handler(request.params);
      this.write({ jsonrpc: '2.0', id: request.id, result } satisfies JSONRPCResult);
    } catch (error) {
      this.write({
        jsonrpc: '2.0',
        id: request.id,
        error: { code: -32000, message: error instanceof Error ? error.message : String(error) },
      } satisfies JSONRPCFailure);
    }
  }

  private write(value: JSONRPCRequest | JSONRPCNotification | JSONRPCResult | JSONRPCFailure): void {
    this.socket?.write(`${JSON.stringify(value)}\n`);
  }
}

// 服务端请求 = 同时带 id 与 method。单看 id 会把请求误判成响应（旧 bug）。
function isIncomingRequest(message: JSONRPCMessage): message is JSONRPCIncomingRequest {
  return 'id' in message
    && typeof message.id === 'string'
    && 'method' in message
    && typeof message.method === 'string';
}
