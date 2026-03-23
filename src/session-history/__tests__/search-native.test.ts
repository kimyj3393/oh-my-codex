import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile, chmod } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import {
  SESSION_SEARCH_BINARY_ENV,
  SESSION_SEARCH_FORCE_TS_ENV,
  resolveSessionSearchBinaryPath,
  searchSessionHistory,
} from '../search.js';

describe('resolveSessionSearchBinaryPath', () => {
  it('prefers explicit environment override', () => {
    const previous = process.env[SESSION_SEARCH_BINARY_ENV];
    try {
      process.env[SESSION_SEARCH_BINARY_ENV] = '/custom/session-search';
      assert.equal(
        resolveSessionSearchBinaryPath({
          cwd: '/repo',
          env: process.env,
          exists: () => false,
        }),
        '/custom/session-search',
      );
    } finally {
      if (typeof previous === 'string') process.env[SESSION_SEARCH_BINARY_ENV] = previous;
      else delete process.env[SESSION_SEARCH_BINARY_ENV];
    }
  });
});

describe('searchSessionHistory native bridge', () => {
  it('uses the native binary when explicitly configured', async () => {
    if (process.platform === 'win32') return;

    const cwd = await mkdtemp(join(tmpdir(), 'omx-session-search-native-'));
    const fakeBinary = join(cwd, 'fake-session-search');
    const previousBinary = process.env[SESSION_SEARCH_BINARY_ENV];
    const previousForceTs = process.env[SESSION_SEARCH_FORCE_TS_ENV];

    try {
      await writeFile(
        fakeBinary,
        [
          '#!/bin/sh',
          'printf \'{"query":"native route","searched_files":1,"matched_sessions":1,"results":[{"session_id":"native-session","timestamp":"2026-03-10T12:00:00.000Z","cwd":"/repo/native","transcript_path":"/tmp/native.jsonl","transcript_path_relative":"sessions/2026/03/10/native.jsonl","record_type":"event_msg:user_message","line_number":2,"snippet":"native result"}]}\'',
          '',
        ].join('\n'),
        'utf-8',
      );
      await chmod(fakeBinary, 0o755);
      process.env[SESSION_SEARCH_BINARY_ENV] = fakeBinary;
      delete process.env[SESSION_SEARCH_FORCE_TS_ENV];

      const report = await searchSessionHistory({
        query: 'native route',
        cwd,
        codexHomeDir: join(cwd, '.codex-home'),
      });

      assert.equal(report.results.length, 1);
      assert.equal(report.results[0].session_id, 'native-session');
      assert.equal(report.results[0].snippet, 'native result');
    } finally {
      if (typeof previousBinary === 'string') process.env[SESSION_SEARCH_BINARY_ENV] = previousBinary;
      else delete process.env[SESSION_SEARCH_BINARY_ENV];
      if (typeof previousForceTs === 'string') process.env[SESSION_SEARCH_FORCE_TS_ENV] = previousForceTs;
      else delete process.env[SESSION_SEARCH_FORCE_TS_ENV];
      await rm(cwd, { recursive: true, force: true });
    }
  });
});
