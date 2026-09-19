# Implementation Plan

1. Add dispatcher-owned catalog DTOs and `--console/tasks` / `--console/task` custom ACP updates.
2. Build an isolated VS Code TypeScript workspace with Node Unix-socket ACP client.
3. Register Activity Bar views for task navigation and runtime diagnostics.
4. Open task details in editor WebviewPanels; query metadata then open a dedicated attach observer.
5. Reduce native ACP updates into immutable trace state and render safely.
6. Add unit coverage for reducer semantics and run Go plus extension validation.
