library;

import 'dart:async';

import 'package:flutter/services.dart';
import 'package:just_audio_media_kit/just_audio_media_kit.dart';
import 'package:just_audio_platform_interface/just_audio_platform_interface.dart';
import 'package:logging/logging.dart';
import 'package:media_kit/media_kit.dart';
import 'src/set_property.dart';

/// An [AudioPlayerPlatform] which wraps `package:media_kit`'s [Player]
class MediaKitPlayer extends AudioPlayerPlatform {
  static const kErrorCode = 1;

  /// `package:media_kit`'s [Player]
  late final Player _player;

  /// The subscriptions that have to be disposed
  late final List<StreamSubscription> _streamSubscriptions;

  final _readyCompleter = Completer<void>();

  /// Completes when the player is ready
  Future<void>? _configured;

  Future<void> ready() => _configured ??= _configureNativePlayer();

  Future<void> _configureNativePlayer() async {
    await _readyCompleter.future;
    for (final property in JustAudioMediaKit.nativeProperties.entries) {
      await setProperty(_player, property.key, property.value);
    }
  }

  static final _logger = Logger('MediaKitPlayer');

  final _eventController = StreamController<PlaybackEventMessage>.broadcast();
  final _dataController = StreamController<PlayerDataMessage>.broadcast();

  ProcessingStateMessage _processingState = ProcessingStateMessage.idle;
  Duration _bufferedPosition = Duration.zero;
  Duration _position = Duration.zero;
  Duration? _duration;
  bool _playing = false;
  bool _shuffleModeEnabled = false;
  bool _mediaOpened = false;
  int? _errorCode;
  String? _errorMessage;
  Completer<Duration?>? _loadCompleter;
  String? _loadFailure;
  int _loadGeneration = 0;
  Future<void> _nativeOpenTail = Future<void>.value();

  /// The index that's currently playing
  int _currentIndex = 0;

  /// [LoadRequest.initialPosition] or [seek] request before [Player.play] was called and/or finished loading.
  Duration? _setPosition;

  Media? get _currentMedia {
    var medias = _player.state.playlist.medias;
    if (medias.isEmpty) return null;
    return medias[_player.state.playlist.index];
  }

  MediaKitPlayer(super.id) {
    _player = Player(
        configuration: PlayerConfiguration(
      pitch: JustAudioMediaKit.pitch,
      protocolWhitelist: JustAudioMediaKit.protocolWhitelist,
      title: JustAudioMediaKit.title,
      bufferSize: JustAudioMediaKit.bufferSize,
      logLevel: JustAudioMediaKit.mpvLogLevel,
      ready: () => _readyCompleter.complete(),
    ));

    if (JustAudioMediaKit.prefetchPlaylist) {
      setProperty(_player, 'prefetch-playlist', 'yes');
    }

    _streamSubscriptions = [
      _player.stream.duration.listen((duration) {
        if (_currentMedia?.extras?['overrideDuration'] != null) return;

        if (_setPosition != null && duration.inSeconds > 0) {
          final position = _setPosition!;
          _setPosition = null;
          unawaited(_player.seek(position));
        }
        _updateDuration(duration);
        _updatePlaybackEvent();
      }),
      _player.stream.position.listen((position) {
        _position = position;
        final start = _currentMedia?.start;
        if (start != null) _position -= start;
        if (_position < Duration.zero) _position = Duration.zero;
        _updatePlaybackEvent();
      }),
      _player.stream.buffering.listen((isBuffering) {
        final start = _currentMedia?.start;
        if (!isBuffering && start != null && _bufferedPosition <= start) {
          // Not ready yet, will be triggered by _player.stream.buffer
          return;
        }
        if (_processingState == ProcessingStateMessage.loading) {
          if (!isBuffering && _mediaOpened) {
            _processingState = ProcessingStateMessage.ready;
            if (_loadCompleter?.isCompleted != true) {
              _loadCompleter?.complete(_duration);
            }
          }
        } else if (_processingState != ProcessingStateMessage.completed ||
            isBuffering) {
          _processingState = isBuffering
              ? ProcessingStateMessage.buffering
              : ProcessingStateMessage.ready;
          if (_duration == null) {
            _updateDuration(_player.state.duration);
          }
        }
        _errorCode = null;
        _errorMessage = null;
        _updatePlaybackEvent();
      }),
      _player.stream.buffer.listen((buffer) {
        _bufferedPosition = buffer;
        // Detect ready for clipping audio source
        final start = _currentMedia?.start;
        if (!_player.state.buffering &&
            _mediaOpened &&
            start != null &&
            _bufferedPosition > start) {
          _processingState = ProcessingStateMessage.ready;
          if (_loadCompleter?.isCompleted != true) {
            _loadCompleter?.complete(_duration);
          }
        }
        _updatePlaybackEvent();
      }),
      _player.stream.volume.listen((volume) {
        _dataController.add(PlayerDataMessage(volume: volume / 100.0));
      }),
      _player.stream.completed.listen((completed) {
        // MPV can report completion for the playlist being replaced while a
        // new YTM URL is still loading. It must not advance the new playlist.
        if (!completed ||
            !_mediaOpened ||
            _processingState == ProcessingStateMessage.loading) {
          return;
        }
        _bufferedPosition = _position = Duration.zero;
        final atPlaylistEnd =
            _currentIndex == _player.state.playlist.medias.length - 1;
        // Saturn's audio handler advances through just_audio's shuffle order.
        // Report each shuffled track's end, not only the end of the native
        // playlist, so it can select the next canonical queue index.
        if (_shuffleModeEnabled ||
            (atPlaylistEnd &&
                _player.state.playlistMode == PlaylistMode.none)) {
          _processingState = ProcessingStateMessage.completed;
        }
        _errorCode = null;
        _errorMessage = null;

        _updatePlaybackEvent();
      }),
      _player.stream.error.listen((error) {
        final errorUri = RegExp(r'Failed to open (.*)\.').firstMatch(error)?[1];
        if (errorUri == null || errorUri == _currentMedia?.uri) {
          _processingState = ProcessingStateMessage.idle;
          _errorCode = kErrorCode;
          _errorMessage = error;
          _updatePlaybackEvent();
          final pendingLoad = _loadCompleter;
          if (pendingLoad != null && !pendingLoad.isCompleted) {
            _loadFailure = error;
            pendingLoad.complete(null);
          }
        }
        _logger.severe('ERROR OCCURRED: $error');
      }),
      _player.stream.playlist.listen((playlist) {
        if (_currentIndex != playlist.index) {
          _bufferedPosition = _position = Duration.zero;
          _currentIndex = playlist.index;
        }
        _duration = _currentMedia?.extras?['overrideDuration'];
        _updatePlaybackEvent();
      }),
      _player.stream.playlistMode.listen((playlistMode) {
        _dataController.add(
            PlayerDataMessage(loopMode: _playlistModeToLoopMode(playlistMode)));
      }),
      _player.stream.pitch.listen((pitch) {
        _dataController.add(PlayerDataMessage(pitch: pitch));
      }),
      _player.stream.rate.listen((rate) {
        _dataController.add(PlayerDataMessage(speed: rate));
      }),
      _player.stream.log.listen((event) {
        // ignore: avoid_print
        print('MPV: [${event.level}] ${event.prefix}: ${event.text}');
      }),
    ];
  }

  void _updateDuration(Duration duration) {
    final start = _currentMedia?.start;
    final end = _currentMedia?.end;
    if (end != null) duration = end;
    if (start != null) duration -= start;
    _duration = duration;
  }

  PlaylistMode _loopModeToPlaylistMode(LoopModeMessage loopMode) {
    return switch (loopMode) {
      LoopModeMessage.off => PlaylistMode.none,
      LoopModeMessage.one => PlaylistMode.single,
      LoopModeMessage.all => PlaylistMode.loop,
    };
  }

  LoopModeMessage _playlistModeToLoopMode(PlaylistMode playlistMode) {
    return switch (playlistMode) {
      PlaylistMode.none => LoopModeMessage.off,
      PlaylistMode.single => LoopModeMessage.one,
      PlaylistMode.loop => LoopModeMessage.all,
    };
  }

  @override
  Stream<PlaybackEventMessage> get playbackEventMessageStream =>
      _eventController.stream;

  @override
  Stream<PlayerDataMessage> get playerDataMessageStream =>
      _dataController.stream;

  /// Updates the playback event
  void _updatePlaybackEvent() {
    _eventController.add(PlaybackEventMessage(
      processingState: _processingState,
      updateTime: DateTime.now(),
      updatePosition: _position,
      bufferedPosition: _bufferedPosition,
      duration: _duration,
      icyMetadata: null,
      currentIndex: _currentIndex,
      androidAudioSessionId: null,
      errorCode: _errorCode,
      errorMessage: _errorMessage,
    ));
  }

  @override
  Future<LoadResponse> load(LoadRequest request) async {
    _logger.finest('load(${request.toMap()})');
    // A new playlist open interrupts the previous one. Release its waiter so
    // just_audio can discard that load instead of keeping it pending forever.
    final previousLoad = _loadCompleter;
    if (previousLoad != null && !previousLoad.isCompleted) {
      previousLoad.complete(null);
    }
    final generation = ++_loadGeneration;
    _loadFailure = null;
    _mediaOpened = false;
    final loadCompleter = _loadCompleter = Completer<Duration?>();
    _currentIndex = request.initialIndex ?? 0;
    _bufferedPosition = Duration.zero;
    _position = Duration.zero;
    _duration = null;
    _setPosition = null;
    _processingState = ProcessingStateMessage.loading;
    _errorCode = null;
    _errorMessage = null;
    _updatePlaybackEvent();

    final Playable playable;
    if (request.audioSourceMessage is ConcatenatingAudioSourceMessage) {
      final audioSource =
          request.audioSourceMessage as ConcatenatingAudioSourceMessage;
      playable = Playlist(
        audioSource.children.map(_convertAudioSourceIntoMediaKit).toList(),
        index: _currentIndex,
      );
    } else {
      playable = _convertAudioSourceIntoMediaKit(request.audioSourceMessage);
    }
    // Native MPV serializes playlist opens. Drop superseded loads before they
    // enter that queue, so rapid skips do not open every intermediate song.
    final openRequest = _nativeOpenTail.then((_) async {
      if (generation != _loadGeneration) return;
      await _player.open(playable, play: _playing);
    });
    _nativeOpenTail = openRequest.catchError((Object _, StackTrace __) {});
    await openRequest;
    if (generation != _loadGeneration) {
      return LoadResponse(duration: null);
    }
    _mediaOpened = true;

    if (request.initialPosition != null &&
        request.initialPosition != Duration.zero) {
      _setPosition = _position = request.initialPosition!;
    }

    _updatePlaybackEvent();
    final duration = await loadCompleter.future;
    if (generation != _loadGeneration) {
      return LoadResponse(duration: null);
    }
    if (_loadFailure case final error?) {
      throw PlatformException(code: '$kErrorCode', message: error);
    }
    return LoadResponse(duration: duration);
  }

  @override
  Future<PlayResponse> play(PlayRequest request) async {
    _playing = true;
    if (_mediaOpened) {
      await _player.play();
    }
    return PlayResponse();
  }

  @override
  Future<PauseResponse> pause(PauseRequest request) async {
    _playing = false;
    if (_mediaOpened) {
      await _player.pause();
    }
    return PauseResponse();
  }

  @override
  Future<SetVolumeResponse> setVolume(SetVolumeRequest request) {
    return _player
        .setVolume(request.volume * 100.0)
        .then((value) => SetVolumeResponse());
  }

  @override
  Future<SetSpeedResponse> setSpeed(SetSpeedRequest request) {
    return _player.setRate(request.speed).then((_) => SetSpeedResponse());
  }

  @override
  Future<SetPitchResponse> setPitch(SetPitchRequest request) =>
      _player.setPitch(request.pitch).then((_) => SetPitchResponse());

  @override
  Future<SetLoopModeResponse> setLoopMode(SetLoopModeRequest request) async {
    await _player.setPlaylistMode(_loopModeToPlaylistMode(request.loopMode));
    return SetLoopModeResponse();
  }

  @override
  Future<SetShuffleModeResponse> setShuffleMode(
      SetShuffleModeRequest request) async {
    _shuffleModeEnabled = request.shuffleMode != ShuffleModeMessage.none;
    // Native shuffle chooses a different random order than just_audio, while
    // the handler and queue screen use just_audio's effectiveIndices.
    await _player.setShuffle(false);
    _dataController.add(PlayerDataMessage(
        shuffleMode: _shuffleModeEnabled
            ? ShuffleModeMessage.all
            : ShuffleModeMessage.none));
    return SetShuffleModeResponse();
  }

  @override
  Future<SetShuffleOrderResponse> setShuffleOrder(
      SetShuffleOrderRequest request) async {
    // The handler uses the order in this request to choose the next track
    // when this adapter reports completion. MPV keeps canonical indexes so
    // seek(index) and queue edits continue to address the same source.
    return SetShuffleOrderResponse();
  }

  @override
  Future<SeekResponse> seek(SeekRequest request) async {
    _logger.finest('seek(${request.toMap()})');
    if (request.index != null) {
      await _player.jump(request.index!);
      if (!_playing) await _player.pause();
    }

    final position = request.position;
    if (position != null) {
      _position = position;

      final start = _currentMedia?.start;
      var nativePosition = position;
      if (start != null) nativePosition += start;
      // jump(index) already starts at zero. A second native seek races the
      // next playlist load when the user skips through YTM tracks quickly.
      if (request.index != null && nativePosition == Duration.zero) {
        _setPosition = null;
      } else if (_player.state.duration.inSeconds > 0) {
        _setPosition = null;
        await _player.seek(nativePosition);
      } else {
        _setPosition = nativePosition;
      }
    } else {
      _position = Duration.zero;
    }

    // reset position on seek
    _updatePlaybackEvent();
    return SeekResponse();
  }

  @override
  Future<ConcatenatingInsertAllResponse> concatenatingInsertAll(
      ConcatenatingInsertAllRequest request) async {
    // _logger.fine('concatenatingInsertAll(${request.toMap()})');
    final originalLength = _player.state.playlist.medias.length;
    for (var i = 0; i < request.children.length; i++) {
      final source = request.children[i];
      await _player.add(_convertAudioSourceIntoMediaKit(source));

      final length = _player.state.playlist.medias.length;

      // media_kit indexes the newly appended source at length - 1. The old
      // code passed `length`, which is one past the end, to playlist-move.
      // Insert each batch item after the previously inserted item.
      final target = request.index < 0
          ? length - 1
          : request.index + i;
      final sourceIndex = originalLength + i;
      if (target >= 0 && target < length - 1 && sourceIndex < length) {
        await _player.move(sourceIndex, target);
      }
    }

    return ConcatenatingInsertAllResponse();
  }

  @override
  Future<ConcatenatingRemoveRangeResponse> concatenatingRemoveRange(
      ConcatenatingRemoveRangeRequest request) async {
    for (var i = request.startIndex; i < request.endIndex; i++) {
      await _player.remove(request.startIndex);
    }

    return ConcatenatingRemoveRangeResponse();
  }

  @override
  Future<ConcatenatingMoveResponse> concatenatingMove(
      ConcatenatingMoveRequest request) {
    return _player
        .move(
            request.currentIndex,
            // not sure why, but apparently there's an underlying difference between just_audio's move implementation
            // and media_kit, so let's fix it
            request.currentIndex > request.newIndex
                ? request.newIndex
                : request.newIndex + 1)
        .then((_) => ConcatenatingMoveResponse());
  }

  /// Release the resources used by this player.
  Future<void> release() async {
    _logger.info('releasing player resources');
    _mediaOpened = false;
    await _player.dispose();
    // cancel all stream subscriptions
    for (final StreamSubscription subscription in _streamSubscriptions) {
      unawaited(subscription.cancel());
    }
    _streamSubscriptions.clear();
  }

  /// Converts an [AudioSourceMessage] into a [Media] for playback
  Media _convertAudioSourceIntoMediaKit(AudioSourceMessage audioSource) {
    switch (audioSource) {
      case final UriAudioSourceMessage uriSource:
        return Media(uriSource.uri, httpHeaders: audioSource.headers);

      // removed because it doesn't seem to be actually working.
      // Related media-kit issue: https://github.com/media-kit/media-kit/issues/28
      // case final SilenceAudioSourceMessage silenceSource:
      //   // from https://github.com/bleonard252/just_audio_mpv/blob/main/lib/src/mpv_player.dart#L137
      //   return Media(
      //     'av://lavfi:anullsrc=d=${silenceSource.duration.inMilliseconds}ms',
      //     extras: {'overrideDuration': silenceSource.duration},
      //   );

      case final ClippingAudioSourceMessage clippingSource:
        return Media(clippingSource.child.uri,
            start: clippingSource.start, end: clippingSource.end);

      default:
        throw UnsupportedError(
            '${audioSource.runtimeType} is currently not supported');
    }
  }
}
