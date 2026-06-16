import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const scriptPath = resolve(dirname(fileURLToPath(import.meta.url)), "bump-version.mjs");

function writeFixture(root, version = "1.2.3", labels = []) {
  mkdirSync(join(root, "src-tauri"));
  writeFileSync(
    join(root, "package.json"),
    `${JSON.stringify({ name: "milena", version }, null, 2)}\n`,
  );
  writeFileSync(
    join(root, "package-lock.json"),
    `${JSON.stringify(
      {
        name: "milena",
        version,
        lockfileVersion: 3,
        packages: {
          "": { name: "milena", version },
        },
      },
      null,
      2,
    )}\n`,
  );
  writeFileSync(
    join(root, "src-tauri", "tauri.conf.json"),
    `${JSON.stringify({ productName: "Milena", version }, null, 2)}\n`,
  );
  writeFileSync(
    join(root, "src-tauri", "Cargo.toml"),
    `[package]\nname = "milena"\nversion = "${version}"\nedition = "2021"\n`,
  );
  writeFileSync(
    join(root, "event.json"),
    `${JSON.stringify({ pull_request: { labels: labels.map((name) => ({ name })) } })}\n`,
  );
}

function runBump(labels) {
  const root = mkdtempSync(join(tmpdir(), "milena-version-"));
  try {
    writeFixture(root, "1.2.3", labels);

    execFileSync(process.execPath, [scriptPath, "--event", "event.json"], {
      cwd: root,
      stdio: "pipe",
    });

    return {
      packageJson: JSON.parse(readFileSync(join(root, "package.json"), "utf8")),
      packageLock: JSON.parse(readFileSync(join(root, "package-lock.json"), "utf8")),
      tauriConfig: JSON.parse(readFileSync(join(root, "src-tauri", "tauri.conf.json"), "utf8")),
      cargoToml: readFileSync(join(root, "src-tauri", "Cargo.toml"), "utf8"),
    };
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

describe("bump-version", () => {
  it("bumps major when the merged pull request has a major label", () => {
    const result = runBump(["major"]);

    expect(result.packageJson.version).toBe("2.0.0");
    expect(result.packageLock.version).toBe("2.0.0");
    expect(result.packageLock.packages[""].version).toBe("2.0.0");
    expect(result.tauriConfig.version).toBe("2.0.0");
    expect(result.cargoToml).toContain('version = "2.0.0"');
  });

  it("bumps patch when the merged pull request has a patch or hotfix label", () => {
    expect(runBump(["patch"]).packageJson.version).toBe("1.2.4");
    expect(runBump(["hotfix"]).packageJson.version).toBe("1.2.4");
  });

  it("bumps minor when no release label is present", () => {
    expect(runBump(["feature"]).packageJson.version).toBe("1.3.0");
  });
});
