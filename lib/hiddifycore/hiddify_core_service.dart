import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:fpdart/fpdart.dart';
import 'package:grpc/grpc.dart';
import 'package:hiddify/core/directories/directories_provider.dart';
import 'package:hiddify/core/model/directories.dart';
import 'package:hiddify/core/notification/in_app_notification_controller.dart';
import 'package:hiddify/core/preferences/general_preferences.dart';
import 'package:hiddify/features/connection/model/connection_failure.dart';
import 'package:hiddify/features/settings/data/config_option_repository.dart';
import 'package:hiddify/hiddifycore/core_interface/core_interface.dart';
import 'package:hiddify/hiddifycore/generated/v2/hcommon/common.pb.dart';
import 'package:hiddify/hiddifycore/generated/v2/hcore/hcore.pb.dart';
import 'package:hiddify/hiddifycore/generated/v2/hcore/hcore_service.pbgrpc.dart';
import 'package:hiddify/hiddifycore/init_signal.dart';
import 'package:hiddify/singbox/model/singbox_config_option.dart';
import 'package:hiddify/features/log/model/log_level.dart' as config_log_level;
import 'package:hiddify/singbox/model/core_status.dart';
import 'package:hiddify/singbox/model/warp_account.dart';

import 'package:hiddify/hiddifycore/core_interface/core_interface_wrapper_stub.dart'
    if (dart.library.io) 'package:hiddify/hiddifycore/core_interface/core_interface_wrapper.dart';
import 'package:hiddify/utils/custom_loggers.dart';
import 'package:hiddify/utils/platform_utils.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:loggy/loggy.dart' as loggyl;
import 'package:riverpod_annotation/riverpod_annotation.dart';
import 'package:rxdart/rxdart.dart';

// The Flutter settings model stores route rules in a nested protobuf JSON
// object. Core's HiddifyOptions parser uses Go JSON tags and numeric protobuf
// enum values, so normalize this payload at the RPC boundary before sending it.
Map<String, dynamic> _settingsJsonForCore(SingboxConfigOption options) {
  final settingsJson = Map<String, dynamic>.from(options.toJson());
  final routeRule = settingsJson.remove('route-rule');
  if (routeRule is Map) {
    // Core v4.1 expects user rules at the top-level `rules` field.
    settingsJson['rules'] = _coreRouteRules(routeRule['rules']);
  }
  return settingsJson;
}

List<Map<String, dynamic>> _coreRouteRules(dynamic value) {
  if (value is! List) return const [];
  return value.whereType<Map>().map(_coreRouteRule).toList();
}

Map<String, dynamic> _coreRouteRule(Map rule) {
  final result = <String, dynamic>{};

  // Protobuf JSON uses lowerCamelCase; Core expects the snake_case names in
  // v2/config/route_rule.pb.go. Supporting both forms also keeps imports from
  // older app versions readable.
  void copy(String coreKey, List<String> jsonKeys) {
    for (final jsonKey in jsonKeys) {
      if (rule.containsKey(jsonKey)) {
        result[coreKey] = rule[jsonKey];
        return;
      }
    }
  }

  copy('list_order', ['listOrder', 'list_order']);
  copy('enabled', ['enabled']);
  copy('name', ['name']);
  copy('rule_sets', ['ruleSets', 'rule_sets', 'ruleSet', 'rule_set']);
  copy('package_names', ['packageNames', 'package_names', 'packageName', 'package_name']);
  copy('process_names', ['processNames', 'process_names', 'processName', 'process_name']);
  copy('process_paths', ['processPaths', 'process_paths', 'processPath', 'process_path']);
  copy('port_ranges', ['portRanges', 'port_ranges', 'portRange', 'port_range']);
  copy('source_port_ranges', ['sourcePortRanges', 'source_port_ranges', 'sourcePortRange', 'source_port_range']);
  copy('ip_cidrs', ['ipCidrs', 'ip_cidrs', 'ipCidr', 'ip_cidr']);
  copy('source_ip_cidrs', ['sourceIpCidrs', 'source_ip_cidrs', 'sourceIpCidr', 'source_ip_cidr']);
  copy('domains', ['domains', 'domain']);
  copy('domain_suffixes', ['domainSuffixes', 'domain_suffixes', 'domainSuffix', 'domain_suffix']);
  copy('domain_keywords', ['domainKeywords', 'domain_keywords', 'domainKeyword', 'domain_keyword']);
  copy('domain_regexes', ['domainRegexes', 'domain_regexes', 'domainRegex', 'domain_regex']);

  // Go's encoding/json cannot decode protobuf enum names into enum integers.
  if (rule.containsKey('outbound')) {
    result['outbound'] = _enumNumber(rule['outbound'], const {
      'proxy': 0,
      'direct': 1,
      'direct_with_fragment': 2,
      'directWithFragment': 2,
      'block': 3,
    });
  }
  if (rule.containsKey('network')) {
    result['network'] = _enumNumber(rule['network'], const {'all': 0, 'tcp': 1, 'udp': 2});
  }
  final protocols = rule['protocols'] ?? rule['protocol'];
  if (protocols is List) {
    result['protocols'] = protocols
        .map(
          (value) => _enumNumber(value, const {'tls': 0, 'http': 1, 'quic': 2, 'stun': 3, 'dns': 4, 'bittorrent': 5}),
        )
        .toList();
  }
  return result;
}

dynamic _enumNumber(dynamic value, Map<String, int> names) {
  if (value is num) return value;
  if (value is String) return names[value] ?? int.tryParse(value) ?? value;
  return value;
}

class HiddifyCoreService with InfraLogger {
  HiddifyCoreService(this.ref);
  final Ref ref;

  // CoreHiddifyCoreService() {}
  final core = getCoreInterface();

  CoreStatus currentState = const CoreStatus.stopped();
  final statusController = BehaviorSubject<CoreStatus>();
  final logController = BehaviorSubject<List<LogMessage>>();
  final CallOptions? grpcOptions = null; //CallOptions(timeout: const Duration(milliseconds: 10000));
  final Map<String, StreamSubscription?> subscriptions = {};
  List<OutboundGroup> latest = [];

  Future<void> init() async {
    await setup()
        .mapLeft((e) {
          loggy.error(e);
          if (PlatformUtils.isIOS) return;
          statusController.add(const CoreStatus.stopped());
          ref.read(inAppNotificationControllerProvider).showErrorToast(e);
        })
        .map((_) {
          loggy.info("Hiddify-core setup done");
          ref.read(coreRestartSignalProvider.notifier).restart();
        })
        .run();
  }

  /// validates config by path and save it
  ///
  /// [path] is used to save validated config
  /// [tempPath] includes base config, possibly invalid
  /// [debug] indicates if debug mode (avoid in prod)

  TaskEither<String, Unit> validateConfigByPath(String path, String tempPath, bool debug) {
    return TaskEither(() async {
      try {
        final response = await core.fgClient.parse(ParseRequest(tempPath: tempPath, configPath: path, debug: false));
        if (response.responseCode != ResponseCode.OK) return left("${response.responseCode} ${response.message}");
      } catch (e) {
        await setup().run();
        final response = await core.fgClient.parse(ParseRequest(tempPath: tempPath, configPath: path, debug: false));
        if (response.responseCode != ResponseCode.OK) return left("${response.responseCode} ${response.message}");
      }
      return right(unit);
    });
  }

  TaskEither<String, String> generateFullConfigByPath(String path) {
    return TaskEither(() async {
      final response = await core.fgClient.parse(ParseRequest(configPath: path, debug: false));
      if (response.responseCode != ResponseCode.OK) return left("${response.responseCode} ${response.message}");
      return right(response.content);
    });
  }

  TaskEither<String, Unit> setup() {
    return TaskEither(() async {
      try {
        final directories = ref.read(appDirectoriesProvider).requireValue;
        final debug = ref.read(debugModeNotifierProvider);
        final setupResponse = await core.setup(directories, debug, 3);

        if (setupResponse.isNotEmpty) {
          return left(setupResponse);
        }

        await startListeningLogs("fg", core.fgClient);
        // await startListeningStatus("fg", core.fgClient);
        if (!core.isSingleChannel()) {
          await startListeningLogs("bg", core.bgClient);
        }
        statusController.add(currentState);
        await startListeningStatus("bg", core.bgClient);
        // ref.read(coreRestartSignalProvider.notifier).restart();
        return right(unit);
      } catch (e) {
        return left(e.toString());
      }
    });
  }

  TaskEither<String, Unit> changeOptions(SingboxConfigOption options) {
    return TaskEither(() async {
      loggy.debug("changing options");
      // latestOptions = options;
      try {
        final settingsJson = _settingsJsonForCore(options);
        final res = await core.fgClient.changeHiddifySettings(
          ChangeHiddifySettingsRequest(hiddifySettingsJson: jsonEncode(settingsJson)),
        );
        if (res.messageType != MessageType.EMPTY) return left("${res.messageType} ${res.message}");
        await core.bgClient.changeHiddifySettings(
          ChangeHiddifySettingsRequest(hiddifySettingsJson: jsonEncode(settingsJson)),
        );
      } on GrpcError catch (e) {
        if (e.code == StatusCode.unavailable) {
          loggy.debug("background core is not started yet! $e");
        } else {
          rethrow;
        }
      }

      return right(unit);
    });
  }

  TaskEither<ConnectionFailure, Unit> start(String path, String name, bool disableMemoryLimit) {
    return TaskEither(() async {
      statusController.add(currentState = const CoreStatus.starting());
      loggy.debug("starting");
      final background = await core.setupBackground(path, name);
      if (background != const CoreStatus.started()) {
        statusController.add(currentState = const CoreStatus.stopped());
        return left(background.getCoreAlert() ?? const ConnectionFailure.unexpected("failed to start core"));
      }
      if (!core.isSingleChannel()) {
        await startListeningLogs("bg", core.bgClient);
        await startListeningStatus("bg", core.bgClient);
      }
      // if (latestOptions != null) {
      //   await core.bgClient.changeHiddifySettings(
      //     ChangeHiddifySettingsRequest(
      //       hiddifySettingsJson: jsonEncode(latestOptions!.toJson()),
      //     ),
      //   );
      // }
      // final content = await File(path).readAsString();
      // loggy.debug("starting with content: $content");
      final request = StartRequest(
        configPath: path,
        configName: name,
        // configContent: content,
        disableMemoryLimit: disableMemoryLimit,
      );
      late CoreInfoResponse res;
      try {
        res = await core.bgClient.start(request);
      } on GrpcError catch (firstError) {
        // On Windows the in-process Core can finish starting while the first
        // gRPC response is lost as its network interfaces are reconfigured.
        // Retrying is safe: Core returns ALREADY_STARTED when that happened.
        loggy.warning("first background core start RPC failed; retrying", firstError);
        await Future<void>.delayed(const Duration(milliseconds: 300));
        try {
          res = await core.bgClient.start(request);
        } on GrpcError catch (retryError) {
          loggy.error("background core start RPC failed twice", retryError);
          ref.read(coreRestartSignalProvider.notifier).restart();
          final detail = "${retryError.codeName}: ${retryError.message ?? retryError.toString()}";
          return left(ConnectionFailure.unexpected("failed to start background core ($detail)"));
        }
      }

      ref.read(coreRestartSignalProvider.notifier).restart();
      if (res.messageType != MessageType.ALREADY_STARTED && res.messageType != MessageType.EMPTY) {
        final alert = res.message.contains("denied") ? CoreAlert.requestVPNPermission : CoreAlert.startFailed;
        currentState = CoreStatus.stopped(
          alert: alert,
          message: "failed to start core ${res.messageType} ${res.message}",
        );

        statusController.add(currentState);

        return left(
          currentState.getCoreAlert() ??
              ConnectionFailure.unexpected("failed to start core ${res.messageType} ${res.message}"),
        );
      }

      // if (res.messageType != MessageType.EMPTY) return left(res);

      return right(unit);
    });
  }

  TaskEither<String, Unit> stop() {
    return TaskEither(() async {
      loggy.debug("stopping");
      var errMsg = "";
      try {
        final res = await core.bgClient.stop(Empty());
      } on GrpcError catch (e) {
        if (e.code == StatusCode.unknown && !(e.message?.contains("HTTP/2") ?? false)) {
          errMsg = e.message ?? "failed to stop core: $e";

          loggy.error("failed to stop bg core: $e");
        }
      } catch (e) {
        loggy.error("failed to stop bg core: $e");
        // left("failed to stop core: $e");
      }
      if (!await core.stop()) {}
      statusController.add(currentState = const CoreStatus.stopped());
      if (errMsg.isNotEmpty) return left(errMsg);
      return right(unit);
    });
  }

  TaskEither<String, Unit> restart(String path, String name, bool disableMemoryLimit) {
    return TaskEither(() async {
      loggy.debug("restarting");
      // if (!await core.restart(path, name)) {
      try {
        final res = await core.bgClient.restart(
          StartRequest(configPath: path, configName: name, disableMemoryLimit: disableMemoryLimit, delayStart: true),
        );
        if (res.messageType != MessageType.EMPTY) return left("${res.messageType} ${res.message}");
      } on GrpcError catch (e) {
        loggy.error("failed to restart bg core: $e");
        if (e.code == StatusCode.unknown && !(e.message?.contains("HTTP/2 error") ?? false)) {
          return left("${e.message}");
        }
      }

      return right(unit);
      // await stop().run();
      // return await start(path, name, disableMemoryLimit).run();
      // }
      // if (!core.isSingleChannel()) {
      //   await startListeningStatus("bg", core.bgClient);
      //   await startListeningLogs("bg", core.bgClient);
      // }
      // return right(unit);
    });
  }

  TaskEither<String, Unit> resetTunnel() {
    return TaskEither(() async {
      // only available on iOS (and macOS later)
      if (!PlatformUtils.isIOS) {
        throw UnimplementedError("reset tunnel function unavailable on platform");
      }

      // loggy.debug("resetting tunnel");
      final res = await core.resetTunnel();
      if (res) {
        return right(unit);
      }
      return left("failed to reset tunnel");
    });
  }

  // Stream<List<OutboundGroup>> watchGroups() async* {
  //   loggy.debug("watching groups");
  //   yield* core.bgClient.outboundsInfo(Empty()).map((event) => event.items);
  //   // res?.cancel();
  // }

  Stream<OutboundGroup?> watchGroup() async* {
    loggy.debug("watching group");
    // interrupt managed by core

    if (!core.isInitialized()) {
      loggy.debug("core is not initialized, returning empty group stream");
      return;
    }
    try {
      yield* core.bgClient.outboundsInfo(Empty()).map((event) => event.items.isEmpty ? null : event.items.first);
    } catch (e) {
      loggy.error("error watching group: $e");
      rethrow;
    }
    // //emitting first event immediately
    // yield* core.bgClient.outboundsInfo(Empty()).take(1).map((event) => event.items.isEmpty ? null : event.items.first);
    // //emitting other event after every 4 seconds(latest event)
    // yield* core.bgClient.outboundsInfo(Empty()).throttleTime(const Duration(seconds: 4), leading: false, trailing: true).map((event) => event.items.isEmpty ? null : event.items.first);
  }

  Stream<List<OutboundGroup>> watchActiveGroups() async* {
    loggy.info("watching active groups");

    if (!core.isInitialized()) {
      loggy.debug("core is not initialized, returning empty group stream");
      return;
    }

    try {
      yield* core.bgClient
          .mainOutboundsInfo(Empty())
          .map((event) {
            return latest = event.items.toList();
          })
          .startWith(latest);
    } catch (e) {
      loggy.error("error watching active groups: $e");
      rethrow;
    }
  }

  //
  // Stream<SingboxStatus> watchStatus() => _status;

  ResponseStream<SystemInfo> watchStats() {
    loggy.debug("watching stats");
    try {
      return core.bgClient.getSystemInfoStream(Empty());
    } catch (e) {
      loggy.error("error watching stats: $e");
      rethrow;
    }
  }

  TaskEither<String, Unit> selectOutbound(String groupTag, String outboundTag) {
    return TaskEither(() async {
      loggy.debug("selecting outbound");
      try {
        final res = await core.bgClient.selectOutbound(
          SelectOutboundRequest(groupTag: groupTag, outboundTag: outboundTag),
          options: CallOptions(timeout: const Duration(seconds: 1)),
        );
        if (res.code != ResponseCode.OK) return left("${res.code} ${res.message}");

        return right(unit);
      } catch (e) {
        loggy.error("error selecting outbound: $e");
        rethrow;
      }
    });
  }

  TaskEither<String, Unit> urlTest(String tag) {
    return TaskEither(() async {
      loggy.debug("url test");
      try {
        final res = await core.bgClient.urlTest(UrlTestRequest(tag: tag));
        if (res.code != ResponseCode.OK) return left("${res.code} ${res.message}");

        return right(unit);
      } catch (e) {
        loggy.error("error in url test: $e");
        rethrow;
      }
    });
  }

  List<LogMessage> logBuffer = [];

  // SingboxConfigOption? latestOptions;

  Stream<List<LogMessage>> watchLogs(String path) async* {
    if (!core.isInitialized()) {
      loggy.debug("core is not initialized, returning empty log stream");
      return;
    }
    await startListeningLogs("bg", core.bgClient);
    await startListeningLogs("fg", core.fgClient);
    try {
      yield* logController.stream;
    } catch (e) {
      loggy.error("error watching logs: $e");
      rethrow;
    }
    // Stream<List<String>> logStream(CoreClient coreClient) {
    //   return coreClient.logListener(Empty()).asBroadcastStream().map((event) => [event.message]).onErrorResume((error, stackTrace) {
    //     loggy.debug('Error in $coreClient: $error, retrying...');
    //     final delay = (currentState == const SingboxStatus.stopped()) ? 5 : 1;
    //     return const Stream<List<String>>.empty().delay(Duration(seconds: delay)).concatWith([logStream(coreClient)]);
    //   });
    // }

    // // Create streams for both fg and bg clients with retry logic
    // final fgLogStream = logStream(core.fgClient);

    // if (core.bgClient == core.fgClient) {
    //   yield* fgLogStream;
    //   return;
    // }
    // final bgLogStream = logStream(core.bgClient);
    // yield* MergeStream([bgLogStream, fgLogStream]);
  }

  TaskEither<String, Unit> clearLogs() {
    return TaskEither(() async {
      loggy.debug("clearing logs");
      logBuffer.clear();
      // final res = await core.bgClient(Empty());
      // if (res.code != ResponseCode.OK) return left("${res.code} ${res.message}");
      return right(unit);
    });
  }

  // TaskEither<String, WarpResponse> generateWarpConfig({
  //   required String licenseKey,
  //   required String previousAccountId,
  //   required String previousAccessToken,
  // }) {
  //   return TaskEither(() async {
  //     loggy.debug("generating warp config");
  //     final warpConfig = await core.fgClient.generateWarpConfig(
  //       GenerateWarpConfigRequest(
  //         licenseKey: licenseKey,
  //         accountId: previousAccountId,
  //         accessToken: previousAccessToken,
  //       ),
  //     );
  //     // if (warpConfig.code != ResponseCode.OK) return left("${warpConfig.code} ${warpConfig.message}");
  //     final WarpResponse warp = (
  //       log: warpConfig.log,
  //       accountId: warpConfig.account.accountId,
  //       accessToken: warpConfig.account.accessToken,
  //       wireguardConfig: jsonEncode(warpConfig.config.toProto3Json()),
  //     );
  //     return right(warp);
  //   });
  // }

  Stream<CoreStatus> watchStatus() async* {
    await startListeningStatus("bg", core.bgClient);
    yield* statusController.stream;
    // .endWith(const CoreStatus.stopped());
  }

  Future<void> startListeningStatus(String key, CoreClient cc) async {
    await listenSingle<CoreStatus>(
      "${key}StatusListener",
      () => cc
          .coreInfoListener(Empty(), options: grpcOptions)
          .doOnCancel(() {
            loggy.error("status", "Canceld");
            if (currentState == const CoreStatus.started()) currentState = const CoreStatus.stopped();
          })
          .doOnData((event) {
            loggy.debug("status", event);
            if (currentState == const CoreStatus.started()) currentState = const CoreStatus.stopped();
          })
          .doOnDone(() {
            loggy.error("status", "done");
            if (currentState == const CoreStatus.started()) currentState = const CoreStatus.stopped();
          })
          .endWith(CoreInfoResponse(coreState: CoreStates.STOPPED))
          .map((event) {
            currentState = CoreStatus.fromCoreInfo(event);
            statusController.add(currentState);
            return currentState;
          }),
      // .endWith(const CoreStatus.stopped())
      onError: (error) {
        loggy.error("Stream error in ${key}StatusListener: $error");

        // currentState = const CoreStatus.stopped();
        // statusController.add(currentState);

        // startListeningStatus(key, cc);
      },
    );
  }

  Future<void> startListeningLogs(String key, CoreClient cc) async {
    final logLevel = ref.read(ConfigOptions.logLevel);
    final coreLogLevel = getCoreLogLevel(logLevel);
    final listenKey = "${key}LogListener";
    // await stopListenSingle(listenKey);
    await listenSingle<LogMessage>(listenKey, () {
      return cc.logListener(LogRequest(level: coreLogLevel), options: grpcOptions).map((event) {
        // Handle incoming event
        logBuffer.add(event);
        if (logBuffer.length > 300) {
          logBuffer.removeAt(0);
        }
        logController.add(logBuffer);
        // loggy.log(getLogLevel(event.level), event.message);
        event.message.split('\n').forEach((line) {
          loggy.log(getLogLevel(event.level), line);
        });
        return event;
      });
    });
  }

  Future<void> stopListenSingle(String key) async {
    // Collect keys to remove first
    final keysToRemove = subscriptions.entries
        .where((entry) => entry.key.startsWith(key))
        .map((entry) => entry.key)
        .toList();

    // Cancel and remove
    for (final k in keysToRemove) {
      final sub = subscriptions[k];
      await sub?.cancel(); // cancel the subscription

      subscriptions.remove(k);
    }
  }

  Future<StreamSubscription<T>?> listenSingle<T>(
    String key,
    Stream<T> Function() stream, {
    Function(dynamic error)? onError,
  }) async {
    if (subscriptions.containsKey(key)) {
      // return subscriptions[key] as StreamSubscription<T>?;
      await stopListenSingle(key);
    }
    subscriptions[key] = null;
    subscriptions[key] = stream().listen(
      (event) {
        // loggy.debug(event);
      },
      cancelOnError: true,
      onError: (error) {
        loggy.log(loggyl.LogLevel.error, 'Stream error: $error');
        onError?.call(error);
        subscriptions[key]?.cancel();
        subscriptions.remove(key);
      },
    );
    return subscriptions[key] as StreamSubscription<T>?;
  }

  loggyl.LogLevel getLogLevel(LogLevel level) {
    return switch (level) {
      LogLevel.DEBUG => loggyl.LogLevel.debug,
      LogLevel.INFO => loggyl.LogLevel.info,
      LogLevel.WARNING => loggyl.LogLevel.warning,
      LogLevel.ERROR => loggyl.LogLevel.error,
      LogLevel.FATAL => loggyl.LogLevel.error,
      _ => loggyl.LogLevel.info, // Default case
    };
  }

  LogLevel getCoreLogLevel(config_log_level.LogLevel level) {
    return switch (level) {
      config_log_level.LogLevel.trace => LogLevel.TRACE,
      config_log_level.LogLevel.debug => LogLevel.DEBUG,
      config_log_level.LogLevel.info => LogLevel.INFO,
      config_log_level.LogLevel.warn => LogLevel.WARNING,
      config_log_level.LogLevel.error => LogLevel.ERROR,
      config_log_level.LogLevel.fatal => LogLevel.FATAL,
      config_log_level.LogLevel.panic => LogLevel.FATAL,
      _ => LogLevel.INFO, // Default case
    };
  }

  Future<void> closeFront() async {
    if (!core.isInitialized()) {
      return;
    }
    if (!core.isSingleChannel()) {
      await stopListenSingle("fg");
      await stopListenSingle("bg");
      try {
        await core.fgClient.close(CloseRequest(mode: SetupMode.GRPC_NORMAL_INSECURE));
      } catch (e) {}
      try {
        await core.fgClient.close(CloseRequest(mode: SetupMode.GRPC_NORMAL));
      } catch (e) {}
    }
  }

  TaskEither<String, LANIPResponse> getLANIP() {
    return TaskEither(() async {
      try {
        final response = await core.fgClient.getLANIP(Empty());
        return right(response);
      } catch (e) {
        loggy.error("failed to get LAN IP: $e");
        return left(e.toString());
      }
    });
  }
}
