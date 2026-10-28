// Builds every release asset Archon's installer can request, plus checksums.txt.
import { $ } from "bun";

const targets = ["bun-darwin-arm64", "bun-darwin-x64", "bun-linux-x64", "bun-linux-arm64"];
await $`rm -rf dist`.quiet();
await $`mkdir -p dist`.quiet();
for (const target of targets) {
  const osArch = target.replace("bun-", "");
  const exe = `dist/archon-provider-omp-${osArch}${osArch.startsWith("windows") ? ".exe" : ""}`;
  await $`bun build --compile --target=${target} src/main.ts --outfile ${exe}`;
}
const files = ["archon-provider-omp-darwin-arm64", "archon-provider-omp-darwin-x64", "archon-provider-omp-linux-x64", "archon-provider-omp-linux-arm64"];
const lines: string[] = [];
for (const file of files) {
  const out = await $`shasum -a 256 ${`dist/${file}`}`.text();
  lines.push(out.replace(`dist/${file}`, file).trim());
}
await Bun.write("dist/checksums.txt", lines.join("\n") + "\n");
console.log(await Bun.file("dist/checksums.txt").text());
