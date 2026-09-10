class DesktopApp {
  const DesktopApp({required this.name, required this.path});

  /// Display name shown in the settings list.
  final String name;

  /// The identity used by the routing rule. This is deliberately path-only;
  /// executable hashes and package identities are not part of this feature.
  final String path;
}
