import { promises as fs } from 'node:fs';
import {
  NullAcpRpcNdjsonSink,
  SocketAcpAgentTransport,
  SpawnAcpAgentTransport,
  type AcpAgentSpawnConfig,
  type AcpHostFilesystem,
  type AcpSessionHostRuntime,
} from '@htmlgtmk/acp-ui';

// Node fs 包装：承接 ACP client 的 fs/readTextFile、fs/writeTextFile host 请求。
// 权限判定在 daemon 侧，这里只负责读写本身。
const nodeHostFilesystem: AcpHostFilesystem = {
  async readTextFile(path: string): Promise<string> {
    return fs.readFile(path, 'utf8');
  },
  async writeTextFile(path: string, content: string): Promise<void> {
    await fs.writeFile(path, content, 'utf8');
  },
};

// AcpSessionHostRuntime 三件套 + 显式 transport 注入（契约 §2.1/§2.2）：
// socketPath 有值走 daemon unix socket，否则回退 spawn 子进程。
export function createAcpSessionHostRuntime(options: {
  getWorkspaceRoot: () => string | undefined;
}): AcpSessionHostRuntime {
  const { getWorkspaceRoot } = options;
  return {
    hostFilesystem: nodeHostFilesystem,
    rpcNdjsonSink: new NullAcpRpcNdjsonSink(),
    getWorkspaceRoot,
    createAgentTransport: (config) =>
      config.socketPath
        ? new SocketAcpAgentTransport({ socketPath: config.socketPath })
        : new SpawnAcpAgentTransport({ config, getWorkspaceRoot }),
  };
}

// GOWORKER 面板固定连 daemon socket；command/args 仅为满足 spawn config 形状，不会被使用。
export function createGoworkerAgentSpawnConfig(socketPath: string): AcpAgentSpawnConfig {
  return { name: 'goworker', command: 'goworker', args: [], socketPath };
}
