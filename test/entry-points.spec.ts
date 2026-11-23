import { readFileSync } from 'node:fs';
import { dirname, extname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const SRC = resolve(dirname(fileURLToPath(import.meta.url)), '..', 'src');


function reachable(entry: string, seen = new Set<string>()): Set<string> {
  if (seen.has(entry)) {
    return seen;
  }
  seen.add(entry);

  const source = readFileSync(entry, 'utf8');

  for (const match of source.matchAll(
    /(?:^|\n)\s*(?:import|export)\b[^;]*?\sfrom\s["'](\.[^"']+)["']/g,
  )) {
    const specifier = match[1]!;
    const base = resolve(dirname(entry), specifier.replace(/\.js$/, ''));

    for (const candidate of [`${base}.ts`, resolve(base, 'index.ts')]) {
      if (exists(candidate)) {
        reachable(candidate, seen);
        break;
      }
    }
  }

  return seen;
}

const exists = (path: string): boolean => {
  try {
    readFileSync(path, 'utf8');
    return extname(path) === '.ts';
  } catch {
    return false;
  }
};

function peersOf(entry: string): string[] {
  const peers = new Set<string>();

  for (const file of reachable(resolve(SRC, entry))) {
    for (const match of readFileSync(file, 'utf8').matchAll(
      /(?:^|\n)\s*(?:import|export)\b[^;]*?\sfrom\s["']([^."'`][^"']*)["']/g,
    )) {
      peers.add(match[1]!);
    }
  }

  return [...peers].sort();
}

describe('entry points do not load a peer the service may not have', () => {
  it('keeps @aws-sdk/client-sqs off the root barrel', () => {
    // The root is what a service imports for DLQ names, counters and lag —
    // surfaces with no SQS in them.
    expect(peersOf('index.ts')).not.toContain('@aws-sdk/client-sqs');
  });

  it('keeps @aws-sdk/client-sqs off the narrow consumer entry', () => {
    expect(peersOf('nest/consumer.ts')).not.toContain('@aws-sdk/client-sqs');
  });

  it('carries @aws-sdk/client-sqs on the two task entries that need it', () => {
    // Guards the two cases above against passing because the walk found nothing.
    expect(peersOf('tasks/index.ts')).toContain('@aws-sdk/client-sqs');
    expect(peersOf('nest/tasks.module.ts')).toContain('@aws-sdk/client-sqs');
  });

  it('exposes each entry the exports map promises as a real source file', () => {
    const manifest = JSON.parse(
      readFileSync(resolve(SRC, '..', 'package.json'), 'utf8'),
    ) as { exports: Record<string, { import: string }> };

    const fromEntry = (target: string): string =>
      resolve(SRC, target.replace(/^\.\/dist\//, '').replace(/\.js$/, '.ts'));

    for (const [subpath, conditions] of Object.entries(manifest.exports)) {
      const source = fromEntry(conditions.import);

      // `./tasks` and `./nest/consumer` are directories/files of re-exports, so a
      // missing source file means the entry ships something nobody can maintain.
      expect(exists(source), `${subpath} -> ${source}`).toBe(true);
    }
  });
});
