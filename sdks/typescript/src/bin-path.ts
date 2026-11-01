import { createRequire } from "node:module";
import path from "node:path";

const require = createRequire(import.meta.url);

const PLATFORM_MAP: Record<string, string> = {
  "linux-x64": "@chronostrace/cli-linux-x64-musl",
  "linux-arm64": "@chronostrace/cli-linux-arm64-musl",
  "darwin-x64": "@chronostrace/cli-darwin-x64",
  "darwin-arm64": "@chronostrace/cli-darwin-arm64",
  "win32-x64": "@chronostrace/cli-win32-x64-msvc",
};

export function resolveChronosTraceBinary(): string {
  if (process.env.CHRONOSTRACE_BIN_PATH) {
    return process.env.CHRONOSTRACE_BIN_PATH;
  }

    const key = `${process.platform}-${process.arch}`;
    const pkgName = PLATFORM_MAP[key];

    if (pkgName) {
      try {
        const pkgJsonPath = require.resolve(`${pkgName}/package.json`);
        const binName = process.platform === "win32" ? "chronostrace.exe" : "chronostrace";
        return path.join(path.dirname(pkgJsonPath), "bin", binName);
      } catch {
        // Fallback to PATH if optional dependency was skipped
      }
    }

    return process.platform === "win32" ? "chronostrace.exe" : "chronostrace";
}
