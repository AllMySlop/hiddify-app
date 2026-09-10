import 'package:flutter/material.dart';
import 'package:hiddify/core/preferences/general_preferences.dart';
import 'package:hiddify/features/desktop_app_exclusions/data/running_apps_provider.dart';
import 'package:hiddify/features/desktop_app_exclusions/model/desktop_app.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';

class DesktopAppExclusionsPage extends ConsumerWidget {
  const DesktopAppExclusionsPage({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    // This list is persistent configuration, separate from the temporary list
    // of processes returned by runningDesktopAppsProvider.
    final exclusions = ref.watch(Preferences.desktopExcludeApps);
    final apps = ref.watch(runningDesktopAppsProvider);
    return Scaffold(
      appBar: AppBar(
        title: const Text('VPN app exclusions'),
        // Refresh only updates the candidate list. Existing exclusions remain
        // saved even when an application is no longer running.
        actions: [IconButton(onPressed: () => ref.invalidate(runningDesktopAppsProvider), icon: const Icon(Icons.refresh))],
      ),
      body: apps.when(
        loading: () => const Center(child: CircularProgressIndicator()),
        error: (error, _) => Center(child: Text(error.toString())),
        data: (running) => ListView(
          children: [
            const Padding(
              padding: const EdgeInsets.fromLTRB(16, 16, 16, 8),
              child: const Text('Selected applications connect directly to the internet when VPN mode is active.'),
            ),
            if (running.isEmpty)
              const Padding(padding: EdgeInsets.all(24), child: Text('No running applications found.')),
            for (final app in running) _AppTile(app: app, selected: exclusions.contains(app.path), ref: ref),
            if (exclusions.isNotEmpty) ...[
              const Divider(),
              ListTile(
                title: const Text('Saved executable paths'),
                subtitle: Text(exclusions.join('\n')),
              ),
            ],
          ],
        ),
      ),
    );
  }
}

class _AppTile extends StatelessWidget {
  const _AppTile({required this.app, required this.selected, required this.ref});
  final DesktopApp app;
  final bool selected;
  final WidgetRef ref;

  @override
  Widget build(BuildContext context) => CheckboxListTile(
    value: selected,
    title: Text(app.name),
    subtitle: Text(app.path, maxLines: 2, overflow: TextOverflow.ellipsis),
    onChanged: (_) {
      // Store the absolute executable path. The route remains effective for
      // future launches of this executable, not just the current process.
      final current = [...ref.read(Preferences.desktopExcludeApps)];
      if (selected) {
        current.remove(app.path);
      } else if (!current.contains(app.path)) {
        current.add(app.path);
      }
      ref.read(Preferences.desktopExcludeApps.notifier).update(current);
    },
  );
}
