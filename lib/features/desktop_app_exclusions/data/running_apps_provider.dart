import 'dart:convert';
import 'dart:io';

import 'package:hiddify/features/desktop_app_exclusions/model/desktop_app.dart';
import 'package:hiddify/utils/platform_utils.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';

/// Takes a snapshot of executable processes currently visible to the OS.
///
/// This provider is intentionally a one-shot query. It supplies candidates for
/// the settings page; it is not the mechanism that enforces exclusions after
/// an application is launched.
final runningDesktopAppsProvider = FutureProvider<List<DesktopApp>>((ref) async {
  final apps = <String, DesktopApp>{};
  if (PlatformUtils.isWindows) {
    // Win32_Process exposes the full executable path, which is the value the
    // TUN route rule later matches against.
    final command = 'Get-CimInstance Win32_Process | Where-Object { \$_.ExecutablePath } | '
        'Select-Object Name,ExecutablePath | ConvertTo-Json -Compress';
    final result = await Process.run('powershell', [
      '-NoProfile',
      '-Command',
      command,
    ]);
    if (result.exitCode != 0) throw Exception(result.stderr);
    final output = '${result.stdout}'.trim();
    if (output.isEmpty) return [];
    final decoded = jsonDecode(output);
    final rows = decoded is List ? decoded : [decoded];
    for (final row in rows) {
      if (row is! Map) continue;
      final path = '${row['ExecutablePath'] ?? ''}'.trim();
      if (path.isEmpty) continue;
      apps[path.toLowerCase()] = DesktopApp(name: '${row['Name'] ?? path}', path: path);
    }
  } else {
    // Linux exposes the canonical executable through /proc/<pid>/exe. macOS
    // falls back to ps, whose comm output contains the executable path.
    final result = await Process.run('ps', PlatformUtils.isLinux ? ['-axo', 'pid='] : ['-axo', 'comm=']);
    if (result.exitCode != 0) throw Exception(result.stderr);
    for (final line in '${result.stdout}'.split('\n')) {
      var path = line.trim();
      if (PlatformUtils.isLinux && path.isNotEmpty) {
        // Reading the link can fail for exited processes or processes without
        // permission; those entries are simply omitted from the snapshot.
        try {
          path = await File('/proc/$path/exe').resolveSymbolicLinks();
        } catch (_) {
          continue;
        }
      }
      if (path.isEmpty || !path.startsWith('/')) continue;
      apps[path] = DesktopApp(name: path.split('/').last, path: path);
    }
  }
  // Multiple process instances can point at the same executable. De-duplicate
  // them so the user selects one path once.
  final result = apps.values.toList()..sort((a, b) => a.name.toLowerCase().compareTo(b.name.toLowerCase()));
  return result;
});
