use std::{
    io::Error,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    },
    time::{Duration, Instant},
};

use cpal::{
    traits::{DeviceTrait, HostTrait, StreamTrait},
    Device, Sample, SizedSample,
};
use rtrb::{Consumer, Producer, RingBuffer};

use crate::audio_toolkit::{
    audio::{AudioVisualiser, FrameResampler},
    constants,
    vad::{self, VadFrame},
    VoiceActivityDetector,
};

enum Cmd {
    /// Begin capturing. Carries the send timestamp so the consumer can log how
    /// long the command sat in the channel, a capture generation for stale
    /// callback suppression, and a one-shot first-sample acknowledgement.
    Start(VadPolicy, Instant, u64, mpsc::Sender<()>),
    Stop(mpsc::Sender<Vec<f32>>),
    Shutdown,
}

const AUDIO_RING_SECONDS: usize = 5;
const CONSUMER_POLL_INTERVAL: Duration = Duration::from_millis(10);
const HEARTBEAT_CHECK_INTERVAL: Duration = Duration::from_millis(100);
const INITIAL_CALLBACK_TIMEOUT: Duration = Duration::from_secs(10);
const ESTABLISHED_CALLBACK_TIMEOUT: Duration = Duration::from_secs(3);
const STARTUP_MISSED_HEARTBEATS: usize =
    (INITIAL_CALLBACK_TIMEOUT.as_millis() / HEARTBEAT_CHECK_INTERVAL.as_millis()) as usize;
const RUNNING_MISSED_HEARTBEATS: usize =
    (ESTABLISHED_CALLBACK_TIMEOUT.as_millis() / HEARTBEAT_CHECK_INTERVAL.as_millis()) as usize;
const PAUSE_ACK_TIMEOUT: Duration = Duration::from_secs(2);
const STOP_RESPONSE_TIMEOUT: Duration = Duration::from_secs(3);
const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(4);
const WORKER_INIT_TIMEOUT: Duration = Duration::from_secs(12);
pub(crate) const MICROPHONE_READY_TIMEOUT: Duration = Duration::from_secs(12);

// These are behavioral contracts, not merely tuning values. Each outer wait
// must leave time for the inner operation to report its more specific failure.
const _: () = assert!(PAUSE_ACK_TIMEOUT.as_millis() < STOP_RESPONSE_TIMEOUT.as_millis());
const _: () = assert!(STOP_RESPONSE_TIMEOUT.as_millis() < WORKER_SHUTDOWN_TIMEOUT.as_millis());
const _: () = assert!(INITIAL_CALLBACK_TIMEOUT.as_millis() < MICROPHONE_READY_TIMEOUT.as_millis());
const _: () = assert!(INITIAL_CALLBACK_TIMEOUT
    .as_millis()
    .is_multiple_of(HEARTBEAT_CHECK_INTERVAL.as_millis()));
const _: () = assert!(ESTABLISHED_CALLBACK_TIMEOUT
    .as_millis()
    .is_multiple_of(HEARTBEAT_CHECK_INTERVAL.as_millis()));

/// State shared by the real-time input callback and the consumer worker.
///
/// Keep this to atomics only: the callback must never allocate, lock, block, or
/// log. Audio samples themselves travel through the wait-free SPSC ring.
#[derive(Default)]
struct CaptureTransportState {
    pause_requested: AtomicBool,
    pause_acknowledged: AtomicBool,
    callback_heartbeat: AtomicU64,
    overrun_samples: AtomicU64,
}

/// How 16 kHz mono frames should be filtered for one recording session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VadPolicy {
    /// Bypass VAD and forward every frame.
    Disabled,
    /// Current offline-tuned VAD profile.
    Offline,
    /// VAD profile with a longer post-speech tail for streaming-capable models.
    Streaming,
}

/// A single VAD engine plus the two hangover-tail lengths its smoothing wrapper
/// should use. The offline and streaming policies are never active
/// concurrently, so one detector is reconfigured per session (see `Cmd::Start`)
/// rather than kept as two resident engines.
#[derive(Clone)]
struct VadConfig {
    detector: Arc<Mutex<Box<dyn vad::VoiceActivityDetector>>>,
    offline_hangover_frames: usize,
    streaming_hangover_frames: usize,
}

impl VadConfig {
    /// Post-speech hangover tail (in 30 ms frames) for the given policy.
    /// `Disabled` never reaches the detector, so it maps to the offline value.
    fn hangover_for(&self, policy: VadPolicy) -> usize {
        match policy {
            VadPolicy::Streaming => self.streaming_hangover_frames,
            VadPolicy::Offline | VadPolicy::Disabled => self.offline_hangover_frames,
        }
    }
}

/// Callback invoked with each 16 kHz mono frame that passes the active capture
/// policy while recording. Used to feed a live streaming transcription as audio arrives.
pub type AudioFrameCallback = Arc<dyn Fn(u64, &[f32]) + Send + Sync + 'static>;
pub type LevelCallback = Arc<dyn Fn(u64, Vec<f32>) + Send + Sync + 'static>;
pub type CaptureErrorCallback = Arc<dyn Fn(u64, &str) + Send + Sync + 'static>;
pub type CaptureWarningCallback = Arc<dyn Fn(u64, u64) + Send + Sync + 'static>;

pub struct AudioRecorder {
    device: Option<Device>,
    cmd_tx: Option<mpsc::Sender<Cmd>>,
    worker_handle: Option<std::thread::JoinHandle<()>>,
    worker_done_rx: Option<mpsc::Receiver<()>>,
    vad: Option<VadConfig>,
    level_cb: Option<LevelCallback>,
    audio_cb: Option<AudioFrameCallback>,
    capture_error_cb: Option<CaptureErrorCallback>,
    capture_warning_cb: Option<CaptureWarningCallback>,
    /// Set when a worker timed out. Such a recorder must be discarded rather
    /// than reopened because a detached worker may still own its VAD mutex.
    replacement_required: Arc<AtomicBool>,
    /// Which input channel to use. None = average all (original behavior).
    selected_channel: Option<usize>,
    /// Preferred stream config cached per device name. The two HAL property
    /// queries in `get_preferred_config` cost ~40-85ms per open (worse on
    /// USB/Bluetooth), which lands on the keypress->capture path in on-demand
    /// mode. Keyed by name so a system-default change misses naturally;
    /// cleared whenever an open fails so a stale rate/format self-heals on the
    /// caller's retry.
    config_cache: Arc<Mutex<Option<(String, cpal::SupportedStreamConfig)>>>,
    /// Set by cpal when the active input stream can no longer capture.
    stream_error: Arc<AtomicBool>,
}

impl AudioRecorder {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(AudioRecorder {
            device: None,
            cmd_tx: None,
            worker_handle: None,
            worker_done_rx: None,
            vad: None,
            level_cb: None,
            audio_cb: None,
            capture_error_cb: None,
            capture_warning_cb: None,
            replacement_required: Arc::new(AtomicBool::new(false)),
            selected_channel: None,
            config_cache: Arc::new(Mutex::new(None)),
            stream_error: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Attach a single VAD engine, reconfigured per session for the offline vs
    /// streaming hangover tail. The two policies are mutually exclusive within a
    /// recording, so one engine covers both instead of two resident instances.
    pub fn with_vad(
        mut self,
        detector: Box<dyn VoiceActivityDetector>,
        offline_hangover_frames: usize,
        streaming_hangover_frames: usize,
    ) -> Self {
        self.vad = Some(VadConfig {
            detector: Arc::new(Mutex::new(detector)),
            offline_hangover_frames,
            streaming_hangover_frames,
        });
        self
    }

    pub fn with_level_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn(u64, Vec<f32>) + Send + Sync + 'static,
    {
        self.level_cb = Some(Arc::new(cb));
        self
    }

    /// Register a callback that receives real-time 16 kHz frames after the active
    /// VAD policy has been applied. Frames arrive in real time, in order, on the
    /// recorder's consumer thread — keep the callback cheap (e.g. forward to a
    /// channel) so it never stalls capture.
    pub fn with_audio_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn(u64, &[f32]) + Send + Sync + 'static,
    {
        self.audio_cb = Some(Arc::new(cb));
        self
    }

    pub fn with_capture_error_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn(u64, &str) + Send + Sync + 'static,
    {
        self.capture_error_cb = Some(Arc::new(cb));
        self
    }

    pub fn with_capture_warning_callback<F>(mut self, cb: F) -> Self
    where
        F: Fn(u64, u64) + Send + Sync + 'static,
    {
        self.capture_warning_cb = Some(Arc::new(cb));
        self
    }

    pub fn with_selected_channel(mut self, channel: Option<u16>) -> Self {
        self.set_selected_channel(channel);
        self
    }

    pub fn set_selected_channel(&mut self, channel: Option<u16>) {
        self.selected_channel = channel.map(usize::from);
    }

    pub fn open(&mut self, device: Option<Device>) -> Result<(), Box<dyn std::error::Error>> {
        if self.worker_handle.is_some() {
            if !self.needs_reopen() {
                return Ok(()); // already open
            }
            log::warn!("Capture stream failed; rebuilding microphone stream");
            self.close()?;
        }

        if self.replacement_required.load(Ordering::Acquire) {
            return Err(Box::new(Error::other(
                "Recorder worker was unresponsive and must be replaced",
            )));
        }
        self.stream_error.store(false, Ordering::Relaxed);

        let (cmd_tx, cmd_rx) = mpsc::channel::<Cmd>();
        let (init_tx, init_rx) = mpsc::sync_channel::<Result<(), String>>(1);
        let (worker_done_tx, worker_done_rx) = mpsc::sync_channel::<()>(1);

        let host = crate::audio_toolkit::get_cpal_host();
        let device = match device {
            Some(dev) => dev,
            None => host
                .default_input_device()
                .ok_or_else(|| Error::new(std::io::ErrorKind::NotFound, "No input device found"))?,
        };

        let thread_device = device.clone();
        let vad = self.vad.clone();
        // Move the optional level callback into the worker thread
        let level_cb = self.level_cb.clone();
        // Move the optional real-time audio frame callback into the worker thread
        let audio_cb = self.audio_cb.clone();
        let capture_error_cb = self.capture_error_cb.clone();
        let capture_warning_cb = self.capture_warning_cb.clone();
        let selected_channel = self.selected_channel;
        let config_cache = Arc::clone(&self.config_cache);
        let stream_error = Arc::clone(&self.stream_error);

        let worker = std::thread::spawn(move || {
            let transport = Arc::new(CaptureTransportState::default());
            let init_result = (|| -> Result<(cpal::Stream, u32, Consumer<f32>), String> {
                let config_started = Instant::now();
                let device_name = thread_device.name().unwrap_or_default();
                let cached_config = config_cache
                    .lock()
                    .unwrap()
                    .as_ref()
                    .filter(|(name, _)| !device_name.is_empty() && *name == device_name)
                    .map(|(_, cfg)| cfg.clone());
                let config_was_cached = cached_config.is_some();
                let config = match cached_config {
                    Some(cfg) => cfg,
                    None => AudioRecorder::get_preferred_config(&thread_device)
                        .map_err(|e| format!("Failed to fetch preferred config: {e}"))?,
                };
                let config_elapsed = config_started.elapsed();

                let sample_rate = config.sample_rate().0;
                let channels = config.channels() as usize;

                log::info!(
                    "Using device: {:?}\nSample rate: {}\nChannels: {}\nFormat: {:?}",
                    thread_device.name(),
                    sample_rate,
                    channels,
                    config.sample_format()
                );

                if let Some(channel) = selected_channel {
                    if channel < channels {
                        log::info!("Using selected input channel: {}", channel + 1);
                    } else {
                        log::warn!(
                            "Selected input channel {} is out of range for a {}-channel device; averaging all channels instead",
                            channel + 1,
                            channels
                        );
                    }
                } else {
                    log::info!("Averaging all {} input channels", channels);
                }

                let build_started = Instant::now();
                let (stream, sample_consumer) = match config.sample_format() {
                    cpal::SampleFormat::U8 => AudioRecorder::build_stream::<u8>(
                        &thread_device,
                        &config,
                        channels,
                        selected_channel,
                        Arc::clone(&transport),
                        Arc::clone(&stream_error),
                    ),
                    cpal::SampleFormat::I8 => AudioRecorder::build_stream::<i8>(
                        &thread_device,
                        &config,
                        channels,
                        selected_channel,
                        Arc::clone(&transport),
                        Arc::clone(&stream_error),
                    ),
                    cpal::SampleFormat::I16 => AudioRecorder::build_stream::<i16>(
                        &thread_device,
                        &config,
                        channels,
                        selected_channel,
                        Arc::clone(&transport),
                        Arc::clone(&stream_error),
                    ),
                    cpal::SampleFormat::I32 => AudioRecorder::build_stream::<i32>(
                        &thread_device,
                        &config,
                        channels,
                        selected_channel,
                        Arc::clone(&transport),
                        Arc::clone(&stream_error),
                    ),
                    cpal::SampleFormat::F32 => AudioRecorder::build_stream::<f32>(
                        &thread_device,
                        &config,
                        channels,
                        selected_channel,
                        Arc::clone(&transport),
                        Arc::clone(&stream_error),
                    ),
                    sample_format => {
                        return Err(format!("Unsupported sample format: {sample_format:?}"));
                    }
                }
                .map_err(|e| format!("Failed to build input stream: {e}"))?;
                let build_elapsed = build_started.elapsed();

                let play_started = Instant::now();
                stream
                    .play()
                    .map_err(|e| format!("Failed to start microphone stream: {e}"))?;
                log::debug!(
                    "mic worker init: fetch_config={:?} (cached={}) build_stream={:?} play={:?}",
                    config_elapsed,
                    config_was_cached,
                    build_elapsed,
                    play_started.elapsed()
                );

                // The device accepted this config; remember it so the next
                // open skips the HAL property queries entirely.
                if !config_was_cached && !device_name.is_empty() {
                    *config_cache.lock().unwrap() = Some((device_name, config));
                }

                Ok((stream, sample_rate, sample_consumer))
            })();

            match init_result {
                Ok((stream, sample_rate, sample_consumer)) => {
                    let _ = init_tx.send(Ok(()));
                    // Timestamp for the play()-returned -> first-samples gap the
                    // init handshake can't see (hardware dependent).
                    let stream_running_at = Instant::now();
                    // Keep the stream alive while we process samples.
                    run_consumer(
                        sample_rate,
                        vad,
                        sample_consumer,
                        cmd_rx,
                        level_cb,
                        audio_cb,
                        capture_error_cb,
                        capture_warning_cb,
                        transport,
                        Arc::clone(&stream_error),
                        stream_running_at,
                    );
                    drop(stream);
                }
                Err(error_message) => {
                    // A failed open may mean the cached config went stale
                    // (device re-plugged, rate/format changed in the OS).
                    // Drop it so the next attempt re-queries the device.
                    *config_cache.lock().unwrap() = None;
                    log::error!("{error_message}");
                    let _ = init_tx.send(Err(error_message));
                }
            }
            let _ = worker_done_tx.send(());
        });

        match init_rx.recv_timeout(WORKER_INIT_TIMEOUT) {
            Ok(Ok(())) => {
                self.device = Some(device);
                self.cmd_tx = Some(cmd_tx);
                self.worker_handle = Some(worker);
                self.worker_done_rx = Some(worker_done_rx);
                Ok(())
            }
            Ok(Err(error_message)) => {
                let _ = worker.join();
                let kind = if is_microphone_access_denied(&error_message) {
                    std::io::ErrorKind::PermissionDenied
                } else {
                    std::io::ErrorKind::Other
                };
                Err(Box::new(Error::new(kind, error_message)))
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.stream_error.store(true, Ordering::Release);
                self.replacement_required.store(true, Ordering::Release);
                // Dropping the command sender makes a worker that eventually
                // finishes initialization exit before it can capture. The
                // handle is detached because the backend call itself is wedged.
                drop(cmd_tx);
                drop(worker);
                Err(Box::new(Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "Timed out after {:?} initializing the microphone worker",
                        WORKER_INIT_TIMEOUT
                    ),
                )))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = worker.join();
                Err(Box::new(Error::other(
                    "Microphone worker exited during initialization",
                )))
            }
        }
    }

    /// Queue a recording start and return a one-shot receiver that resolves only
    /// after the first real microphone sample chunk has entered the capture path.
    /// `Stream::play()` returning is not sufficient: some Bluetooth and USB
    /// devices take much longer to begin delivering callbacks.
    pub fn start(
        &self,
        vad_policy: VadPolicy,
        capture_generation: u64,
    ) -> Result<mpsc::Receiver<()>, Box<dyn std::error::Error>> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| Error::other("Recorder is not open"))?;
        let (ready_tx, ready_rx) = mpsc::channel();
        tx.send(Cmd::Start(
            vad_policy,
            Instant::now(),
            capture_generation,
            ready_tx,
        ))?;
        Ok(ready_rx)
    }

    pub fn stop(&self) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
        self.stop_with_timeout(STOP_RESPONSE_TIMEOUT)
    }

    fn stop_with_timeout(&self, timeout: Duration) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| Error::other("Recorder is not open"))?;
        let (resp_tx, resp_rx) = mpsc::channel();
        if let Err(error) = tx.send(Cmd::Stop(resp_tx)) {
            self.stream_error.store(true, Ordering::Release);
            self.replacement_required.store(true, Ordering::Release);
            return Err(Box::new(error));
        }
        match resp_rx.recv_timeout(timeout) {
            Ok(samples) => Ok(samples),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.stream_error.store(true, Ordering::Release);
                self.replacement_required.store(true, Ordering::Release);
                Err(Box::new(Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Timed out after {timeout:?} stopping microphone capture"),
                )))
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                self.stream_error.store(true, Ordering::Release);
                self.replacement_required.store(true, Ordering::Release);
                Err(Box::new(Error::other(
                    "Microphone worker exited before returning captured samples",
                )))
            }
        }
    }

    /// True when the active capture stream must be rebuilt.
    ///
    /// cpal may report a device disconnect asynchronously without closing its
    /// callback channel, so also honor the error callback's explicit flag.
    pub fn needs_reopen(&self) -> bool {
        self.stream_error.load(Ordering::Relaxed)
            || self
                .worker_handle
                .as_ref()
                .is_some_and(|handle| handle.is_finished())
    }

    pub fn replacement_required(&self) -> bool {
        self.replacement_required.load(Ordering::Acquire)
    }

    pub fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.close_with_timeout(WORKER_SHUTDOWN_TIMEOUT)
    }

    fn close_with_timeout(&mut self, timeout: Duration) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(tx) = self.cmd_tx.take() {
            let _ = tx.send(Cmd::Shutdown);
        }
        if let Some(handle) = self.worker_handle.take() {
            let worker_finished = if handle.is_finished() {
                self.worker_done_rx = None;
                true
            } else {
                self.worker_done_rx.take().is_some_and(|done| {
                    !matches!(
                        done.recv_timeout(timeout),
                        Err(mpsc::RecvTimeoutError::Timeout)
                    )
                })
            };
            if worker_finished {
                let _ = handle.join();
            } else {
                // Dropping a JoinHandle detaches the worker. This is preferable
                // to freezing Handy forever if a platform audio backend wedges.
                // The owner must replace this recorder so the detached worker
                // cannot share a VAD mutex with the next capture generation.
                self.stream_error.store(true, Ordering::Release);
                self.replacement_required.store(true, Ordering::Release);
                self.device = None;
                log::warn!(
                    "Timed out shutting down microphone worker; detaching wedged audio stream"
                );
                return Err(Box::new(Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Timed out after {timeout:?} shutting down microphone worker"),
                )));
            }
        } else {
            self.worker_done_rx = None;
        }
        self.device = None;
        Ok(())
    }

    fn build_stream<T>(
        device: &cpal::Device,
        config: &cpal::SupportedStreamConfig,
        channels: usize,
        selected_channel: Option<usize>,
        transport: Arc<CaptureTransportState>,
        stream_error: Arc<AtomicBool>,
    ) -> Result<(cpal::Stream, Consumer<f32>), cpal::BuildStreamError>
    where
        T: Sample + SizedSample + Copy + Send + 'static,
        f32: cpal::FromSample<T>,
    {
        let ring_capacity = config.sample_rate().0 as usize * AUDIO_RING_SECONDS;
        let (mut sample_producer, mut sample_consumer) = RingBuffer::new(ring_capacity);

        // rtrb allocates uninitialized storage. Fill and drain it before CoreAudio
        // can invoke the callback so the callback never takes first-touch page
        // faults on ring memory.
        {
            let chunk = sample_producer
                .write_chunk(ring_capacity)
                .expect("new audio ring has its full capacity available");
            chunk.commit_all();
        }
        {
            let chunk = sample_consumer
                .read_chunk(ring_capacity)
                .expect("pre-filled audio ring is readable");
            chunk.commit_all();
        }

        // Resolve the effective channel to use. If the selected channel is
        // out of range for this device, fall back to averaging all channels.
        let use_channel = selected_channel.filter(|&channel| channel < channels);
        let callback_transport = Arc::clone(&transport);
        let stream_cb = move |data: &[T], _: &cpal::InputCallbackInfo| {
            Self::write_input_to_ring(
                data,
                channels,
                use_channel,
                &mut sample_producer,
                &callback_transport,
            );
        };

        let stream = device.build_input_stream(
            &config.clone().into(),
            stream_cb,
            move |_err| {
                // Error callbacks may share the platform audio thread. Defer
                // logging and recovery to the consumer/manager path.
                stream_error.store(true, Ordering::Release);
            },
            None,
        )?;
        Ok((stream, sample_consumer))
    }

    /// Real-time callback body. Keep this allocation-free, wait-free, and free
    /// of locks, logging, clocks, and system calls.
    fn write_input_to_ring<T>(
        data: &[T],
        channels: usize,
        use_channel: Option<usize>,
        producer: &mut Producer<f32>,
        transport: &CaptureTransportState,
    ) where
        T: Sample + SizedSample + Copy,
        f32: cpal::FromSample<T>,
    {
        transport.callback_heartbeat.fetch_add(1, Ordering::Relaxed);

        if transport.pause_requested.load(Ordering::Acquire) {
            transport.pause_acknowledged.store(true, Ordering::Release);
            return;
        }

        let frame_count = data.len() / channels;
        let writable_frames = producer.slots().min(frame_count);
        let written = if writable_frames == 0 {
            0
        } else {
            let chunk = producer
                .write_chunk_uninit(writable_frames)
                .expect("the producer just reported this many writable slots");
            if channels == 1 {
                chunk.fill_from_iter(
                    data.iter()
                        .take(writable_frames)
                        .map(|&sample| sample.to_sample::<f32>()),
                )
            } else if let Some(channel) = use_channel {
                chunk.fill_from_iter(
                    data.chunks_exact(channels)
                        .take(writable_frames)
                        .map(|frame| frame[channel].to_sample::<f32>()),
                )
            } else {
                chunk.fill_from_iter(data.chunks_exact(channels).take(writable_frames).map(
                    |frame| {
                        frame
                            .iter()
                            .map(|&sample| sample.to_sample::<f32>())
                            .sum::<f32>()
                            / channels as f32
                    },
                ))
            }
        };
        debug_assert_eq!(written, writable_frames);

        let dropped = frame_count - written;
        if dropped > 0 {
            transport
                .overrun_samples
                .fetch_add(dropped as u64, Ordering::Relaxed);
        }

        // Stop may be requested after the entry check but before this write
        // commits. A post-write acknowledgment makes that write visible before
        // the consumer's final drain, closing the request-during-write window.
        acknowledge_pause_after_write(transport);
    }

    pub fn preferred_input_channel_count(
        device: &cpal::Device,
    ) -> Result<u16, Box<dyn std::error::Error>> {
        Ok(Self::get_preferred_config(device)?.channels())
    }

    fn get_preferred_config(
        device: &cpal::Device,
    ) -> Result<cpal::SupportedStreamConfig, Box<dyn std::error::Error>> {
        // Use the device's native/default sample rate and let the FrameResampler
        // in run_consumer() downsample to 16kHz. This avoids forcing hardware into
        // a non-native rate which can cause issues on some devices (Bluetooth
        // codecs, certain ALSA drivers, etc.).
        let default_config = device.default_input_config()?;
        let target_rate = default_config.sample_rate();

        // Try to find the best sample format at the device's default rate
        let supported_configs = match device.supported_input_configs() {
            Ok(configs) => configs,
            Err(e) => {
                log::warn!("Could not enumerate input configs ({e}), using device default");
                return Ok(default_config);
            }
        };
        let mut best_config: Option<cpal::SupportedStreamConfigRange> = None;

        for config_range in supported_configs {
            if config_range.min_sample_rate() <= target_rate
                && config_range.max_sample_rate() >= target_rate
            {
                match best_config {
                    None => best_config = Some(config_range),
                    Some(ref current) => {
                        // Prioritize F32 > I16 > I32 > others
                        let score = |fmt: cpal::SampleFormat| match fmt {
                            cpal::SampleFormat::F32 => 4,
                            cpal::SampleFormat::I16 => 3,
                            cpal::SampleFormat::I32 => 2,
                            _ => 1,
                        };

                        if score(config_range.sample_format()) > score(current.sample_format()) {
                            best_config = Some(config_range);
                        }
                    }
                }
            }
        }

        if let Some(config) = best_config {
            return Ok(config.with_sample_rate(target_rate));
        }

        // Fall back to device default if no config matched (exotic/virtual devices)
        log::warn!(
            "No supported config matched device default rate {:?}, using default config",
            target_rate
        );
        Ok(default_config)
    }
}

fn acknowledge_pause_after_write(transport: &CaptureTransportState) {
    if transport.pause_requested.load(Ordering::Acquire) {
        transport.pause_acknowledged.store(true, Ordering::Release);
    }
}

pub fn is_microphone_access_denied(error_message: &str) -> bool {
    let normalized = error_message.to_lowercase();
    normalized.contains("access is denied")
        || normalized.contains("permission denied")
        || normalized.contains("0x80070005")
}

pub fn is_no_input_device_error(error_message: &str) -> bool {
    let normalized = error_message.to_lowercase();
    normalized.contains("no input device found")
        || (normalized.contains("failed to fetch preferred config")
            && normalized.contains("coreaudio"))
}

fn handle_frame(
    samples: &[f32],
    recording: bool,
    capture_generation: Option<u64>,
    vad_policy: VadPolicy,
    vad: &Option<VadConfig>,
    audio_cb: &Option<AudioFrameCallback>,
    out_buf: &mut Vec<f32>,
) {
    if !recording {
        return;
    }

    let Some(generation) = capture_generation else {
        return;
    };

    let mut emit = |buf: &[f32]| {
        out_buf.extend_from_slice(buf);
        if let Some(cb) = audio_cb {
            cb(generation, buf);
        }
    };

    if vad_policy == VadPolicy::Disabled {
        emit(samples);
        return;
    }

    if let Some(cfg) = vad {
        let mut detector = cfg.detector.lock().unwrap();
        match detector
            .push_frame(samples)
            .unwrap_or(VadFrame::Speech(samples))
        {
            VadFrame::Speech(buf) => emit(buf),
            VadFrame::Noise => {}
        }
    } else {
        emit(samples);
    }
}

fn drain_available_samples(consumer: &mut Consumer<f32>, mut process: impl FnMut(&[f32])) -> usize {
    let available = consumer.slots();
    if available == 0 {
        return 0;
    }

    let chunk = consumer
        .read_chunk(available)
        .expect("reported audio ring slots must be readable");
    let (first, second) = chunk.as_slices();
    if !first.is_empty() {
        process(first);
    }
    if !second.is_empty() {
        process(second);
    }
    chunk.commit_all();
    available
}

#[allow(clippy::too_many_arguments)]
fn process_raw_chunk(
    raw: &[f32],
    in_sample_rate: u32,
    recording: bool,
    capture_generation: Option<u64>,
    vad_policy: VadPolicy,
    vad: &Option<VadConfig>,
    audio_cb: &Option<AudioFrameCallback>,
    level_cb: &Option<LevelCallback>,
    processed_samples: &mut Vec<f32>,
    visualizer: &mut AudioVisualiser,
    frame_resampler: &mut FrameResampler,
    first_chunk_logged: &mut bool,
    stream_running_at: Instant,
    awaiting_first_captured_chunk: &mut Option<Instant>,
    capture_ready_tx: &mut Option<mpsc::Sender<()>>,
) {
    let chunk_ms = raw.len() as f64 * 1000.0 / in_sample_rate as f64;
    if !*first_chunk_logged {
        *first_chunk_logged = true;
        log::debug!(
            "first audio samples arrived {:?} after stream start ({:.1}ms drained)",
            stream_running_at.elapsed(),
            chunk_ms
        );
    }

    if !recording {
        return;
    }

    let Some(generation) = capture_generation else {
        return;
    };

    if let Some(buckets) = visualizer.feed(raw) {
        if let Some(callback) = level_cb {
            callback(generation, buckets);
        }
    }

    frame_resampler.push(raw, &mut |frame: &[f32]| {
        handle_frame(
            frame,
            true,
            Some(generation),
            vad_policy,
            vad,
            audio_cb,
            processed_samples,
        )
    });

    if let Some(started) = awaiting_first_captured_chunk.take() {
        log::debug!(
            "first captured samples ({:.1}ms) processed {:?} after Cmd::Start",
            chunk_ms,
            started.elapsed()
        );
    }
    if let Some(ready_tx) = capture_ready_tx.take() {
        // Silence still counts: readiness means the host is delivering samples,
        // not that VAD has detected speech.
        let _ = ready_tx.send(());
    }
}

fn report_capture_failure(
    recording: bool,
    capture_generation: Option<u64>,
    capture_ready_tx: &mut Option<mpsc::Sender<()>>,
    capture_error_cb: &Option<CaptureErrorCallback>,
    detail: &str,
) {
    // Before readiness, dropping the sender wakes the dedicated readiness
    // waiter, which owns startup-failure cleanup. After readiness, notify the
    // manager callback so an established recording is cancelled visibly.
    let recording_was_ready = recording && capture_ready_tx.is_none();
    drop(capture_ready_tx.take());
    if recording_was_ready {
        if let (Some(generation), Some(callback)) = (capture_generation, capture_error_cb) {
            callback(generation, detail);
        }
    }
}

fn observe_recording_overrun(
    samples: u64,
    capture_generation: Option<u64>,
    total_dropped_samples: &mut u64,
    warning_sent: &mut bool,
    capture_warning_cb: &Option<CaptureWarningCallback>,
) {
    if samples == 0 {
        return;
    }

    *total_dropped_samples = total_dropped_samples.saturating_add(samples);
    if !*warning_sent {
        *warning_sent = true;
        log::warn!(
            "Microphone capture ring dropped {samples} samples; continuing the active recording"
        );
        if let (Some(generation), Some(callback)) = (capture_generation, capture_warning_cb) {
            callback(generation, *total_dropped_samples);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_consumer(
    in_sample_rate: u32,
    vad: Option<VadConfig>,
    mut sample_consumer: Consumer<f32>,
    cmd_rx: mpsc::Receiver<Cmd>,
    level_cb: Option<LevelCallback>,
    audio_cb: Option<AudioFrameCallback>,
    capture_error_cb: Option<CaptureErrorCallback>,
    capture_warning_cb: Option<CaptureWarningCallback>,
    transport: Arc<CaptureTransportState>,
    stream_error: Arc<AtomicBool>,
    stream_running_at: Instant,
) {
    let mut frame_resampler = FrameResampler::new(
        in_sample_rate as usize,
        constants::WHISPER_SAMPLE_RATE as usize,
        Duration::from_millis(30),
    );

    let mut processed_samples = Vec::<f32>::new();
    let mut recording = false;
    let mut capture_generation = None;
    let mut total_dropped_samples = 0u64;
    let mut overrun_warning_sent = false;
    let mut vad_policy = VadPolicy::Offline;

    // ---------- latency instrumentation ---------------------------------- //
    // First-chunk arrival exposes the play()->samples-flowing gap; the
    // first-captured log confirms capture begins with the chunk in flight
    // when Cmd::Start lands.
    let mut first_chunk_logged = false;
    let mut awaiting_first_captured_chunk: Option<Instant> = None;
    let mut capture_ready_tx: Option<mpsc::Sender<()>> = None;

    // ---------- spectrum visualisation setup ---------------------------- //
    const BUCKETS: usize = 16;
    // Scale the FFT window to the device sample rate so the analysis window
    // (~33 ms) and frequency resolution (~30 Hz/bin) stay roughly constant
    // across devices. A fixed 512-sample window collapses the low vocal
    // buckets onto a single bin at 48 kHz (e.g. built-in laptop mics), and
    // would stutter at ~4-8 updates/sec on an 8-16 kHz Bluetooth headset.
    // Targets: 48 kHz -> 2048, 16 kHz -> 512, 8 kHz -> 256.
    let target_window = (f64::from(in_sample_rate) / 30.0).round() as usize;
    let window_size = [256usize, 512, 1024, 2048]
        .into_iter()
        .min_by_key(|w| w.abs_diff(target_window))
        .unwrap();
    let mut visualizer = AudioVisualiser::new(
        in_sample_rate,
        window_size,
        BUCKETS,
        400.0,  // vocal_min_hz
        4000.0, // vocal_max_hz
    );

    // The command channel remains blocking because it is never touched by the
    // real-time callback. A short timeout is the consumer's ring-drain tick.
    // Handling commands before draining preserves the old in-flight-buffer
    // behavior at Start without retaining an unbounded pre-roll.
    let mut last_heartbeat = transport.callback_heartbeat.load(Ordering::Relaxed);
    let mut missed_heartbeat_checks = 0usize;
    let mut last_heartbeat_check = Instant::now();

    loop {
        let mut command = match cmd_rx.recv_timeout(CONSUMER_POLL_INTERVAL) {
            Ok(command) => Some(command),
            Err(mpsc::RecvTimeoutError::Timeout) => None,
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        };

        loop {
            if let Some(cmd) = command.take() {
                match cmd {
                    Cmd::Start(policy, sent_at, generation, ready_tx) => {
                        log::debug!(
                            "Cmd::Start processed {:?} after send; capture begins with {} samples",
                            sent_at.elapsed(),
                            if sample_consumer.slots() > 0 {
                                "the in-flight"
                            } else {
                                "the next available"
                            }
                        );
                        awaiting_first_captured_chunk = Some(Instant::now());
                        capture_ready_tx = Some(ready_tx);
                        capture_generation = Some(generation);
                        total_dropped_samples = 0;
                        overrun_warning_sent = false;
                        // Discard overruns accumulated while the always-on ring
                        // was idle; only active-capture loss is user-visible.
                        transport.overrun_samples.store(0, Ordering::Release);
                        vad_policy = policy;
                        processed_samples.clear();
                        recording = true;
                        visualizer.reset();
                        frame_resampler.reset();
                        if vad_policy != VadPolicy::Disabled {
                            if let Some(cfg) = &vad {
                                let mut detector = cfg.detector.lock().unwrap();
                                detector.set_hangover_frames(cfg.hangover_for(vad_policy));
                                detector.reset();
                            }
                        }
                    }
                    Cmd::Stop(reply_tx) => {
                        observe_recording_overrun(
                            transport.overrun_samples.swap(0, Ordering::AcqRel),
                            capture_generation,
                            &mut total_dropped_samples,
                            &mut overrun_warning_sent,
                            &capture_warning_cb,
                        );
                        recording = false;
                        capture_ready_tx = None;
                        awaiting_first_captured_chunk = None;

                        // Release/Acquire on the acknowledgment establishes that
                        // every producer write before the acknowledgment is
                        // visible before the final drain below.
                        transport.pause_acknowledged.store(false, Ordering::Relaxed);
                        transport.pause_requested.store(true, Ordering::Release);
                        let pause_started = Instant::now();
                        while !transport.pause_acknowledged.load(Ordering::Acquire)
                            && pause_started.elapsed() < PAUSE_ACK_TIMEOUT
                        {
                            drain_available_samples(&mut sample_consumer, |raw| {
                                process_raw_chunk(
                                    raw,
                                    in_sample_rate,
                                    true,
                                    capture_generation,
                                    vad_policy,
                                    &vad,
                                    &audio_cb,
                                    &level_cb,
                                    &mut processed_samples,
                                    &mut visualizer,
                                    &mut frame_resampler,
                                    &mut first_chunk_logged,
                                    stream_running_at,
                                    &mut awaiting_first_captured_chunk,
                                    &mut capture_ready_tx,
                                )
                            });
                            std::thread::sleep(Duration::from_millis(1));
                        }

                        let pause_timed_out = !transport.pause_acknowledged.load(Ordering::Acquire);
                        if pause_timed_out {
                            log::warn!("Timed out waiting for the microphone callback to pause");
                            stream_error.store(true, Ordering::Release);
                        }

                        while drain_available_samples(&mut sample_consumer, |raw| {
                            process_raw_chunk(
                                raw,
                                in_sample_rate,
                                true,
                                capture_generation,
                                vad_policy,
                                &vad,
                                &audio_cb,
                                &level_cb,
                                &mut processed_samples,
                                &mut visualizer,
                                &mut frame_resampler,
                                &mut first_chunk_logged,
                                stream_running_at,
                                &mut awaiting_first_captured_chunk,
                                &mut capture_ready_tx,
                            )
                        }) > 0
                        {}

                        frame_resampler.finish(&mut |frame: &[f32]| {
                            handle_frame(
                                frame,
                                true,
                                capture_generation,
                                vad_policy,
                                &vad,
                                &audio_cb,
                                &mut processed_samples,
                            )
                        });
                        if total_dropped_samples > 0 {
                            log::warn!(
                                "Active recording completed after dropping {total_dropped_samples} microphone samples"
                            );
                        }
                        capture_generation = None;
                        let _ = reply_tx.send(std::mem::take(&mut processed_samples));

                        if pause_timed_out {
                            return;
                        }
                        transport.pause_acknowledged.store(false, Ordering::Relaxed);
                        transport.pause_requested.store(false, Ordering::Release);
                    }
                    Cmd::Shutdown => {
                        transport.pause_requested.store(true, Ordering::Release);
                        return;
                    }
                }
            }

            command = match cmd_rx.try_recv() {
                Ok(command) => Some(command),
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => return,
            };
        }

        let recording_now = recording;
        drain_available_samples(&mut sample_consumer, |raw| {
            process_raw_chunk(
                raw,
                in_sample_rate,
                recording_now,
                capture_generation,
                vad_policy,
                &vad,
                &audio_cb,
                &level_cb,
                &mut processed_samples,
                &mut visualizer,
                &mut frame_resampler,
                &mut first_chunk_logged,
                stream_running_at,
                &mut awaiting_first_captured_chunk,
                &mut capture_ready_tx,
            )
        });

        let overrun_samples = transport.overrun_samples.swap(0, Ordering::AcqRel);
        if recording {
            observe_recording_overrun(
                overrun_samples,
                capture_generation,
                &mut total_dropped_samples,
                &mut overrun_warning_sent,
                &capture_warning_cb,
            );
        }

        if stream_error.load(Ordering::Acquire) {
            log::error!("Microphone backend reported a stream error; rebuilding stream");
            report_capture_failure(
                recording,
                capture_generation,
                &mut capture_ready_tx,
                &capture_error_cb,
                "The microphone backend reported an error; the stream will be rebuilt",
            );
            return;
        }

        if last_heartbeat_check.elapsed() >= HEARTBEAT_CHECK_INTERVAL {
            let heartbeat = transport.callback_heartbeat.load(Ordering::Relaxed);
            if heartbeat == last_heartbeat {
                missed_heartbeat_checks += 1;
            } else {
                last_heartbeat = heartbeat;
                missed_heartbeat_checks = 0;
            }
            last_heartbeat_check = Instant::now();

            let missed_limit = if first_chunk_logged {
                RUNNING_MISSED_HEARTBEATS
            } else {
                STARTUP_MISSED_HEARTBEATS
            };
            if missed_heartbeat_checks >= missed_limit {
                log::error!(
                    "Microphone callback stalled after {} consecutive heartbeat checks",
                    missed_heartbeat_checks
                );
                stream_error.store(true, Ordering::Release);
                report_capture_failure(
                    recording,
                    capture_generation,
                    &mut capture_ready_tx,
                    &capture_error_cb,
                    "The microphone stopped delivering audio; the stream will be rebuilt",
                );
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        is_microphone_access_denied, is_no_input_device_error, run_consumer, AudioRecorder,
        CaptureTransportState, Cmd,
    };
    use rtrb::RingBuffer;
    use std::{
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc, Arc,
        },
        thread,
        time::{Duration, Instant},
    };

    #[test]
    fn unopened_recorder_does_not_need_reopen() {
        // No worker has been spawned yet, so there is nothing to reap. Guards
        // against inverting the "no worker" case, which would make every first
        // open() take the rebuild path.
        let recorder = AudioRecorder::new().expect("recorder");
        assert!(!recorder.needs_reopen());
    }

    #[test]
    fn stream_error_requires_reopen() {
        let recorder = AudioRecorder::new().expect("recorder");
        recorder.stream_error.store(true, Ordering::Relaxed);
        assert!(recorder.needs_reopen());
    }

    #[test]
    fn stop_timeout_is_bounded_and_requires_a_fresh_recorder() {
        let mut recorder = AudioRecorder::new().expect("recorder");
        let (cmd_tx, _cmd_rx) = mpsc::channel();
        recorder.cmd_tx = Some(cmd_tx);

        let started = Instant::now();
        let error = recorder
            .stop_with_timeout(Duration::from_millis(20))
            .expect_err("consumer never replies");

        assert_eq!(
            error
                .downcast_ref::<std::io::Error>()
                .map(std::io::Error::kind),
            Some(std::io::ErrorKind::TimedOut)
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(recorder.replacement_required());
    }

    #[test]
    fn shutdown_timeout_detaches_and_requires_a_fresh_recorder() {
        let mut recorder = AudioRecorder::new().expect("recorder");
        let (done_tx, done_rx) = mpsc::channel();
        recorder.worker_done_rx = Some(done_rx);
        recorder.worker_handle = Some(thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            drop(done_tx);
        }));

        let error = recorder
            .close_with_timeout(Duration::from_millis(20))
            .expect_err("worker is intentionally wedged");

        assert_eq!(
            error
                .downcast_ref::<std::io::Error>()
                .map(std::io::Error::kind),
            Some(std::io::ErrorKind::TimedOut)
        );
        assert!(recorder.worker_handle.is_none());
        assert!(recorder.replacement_required());
    }

    #[test]
    fn post_write_pause_check_acknowledges_a_new_request() {
        let transport = CaptureTransportState::default();
        assert!(!transport.pause_acknowledged.load(Ordering::Acquire));

        // Models Stop arriving after the callback's entry check but after its
        // ring commit. The post-write check must acknowledge that request.
        transport.pause_requested.store(true, Ordering::Release);
        super::acknowledge_pause_after_write(&transport);

        assert!(transport.pause_acknowledged.load(Ordering::Acquire));
    }

    #[test]
    fn timeout_ladder_preserves_inner_before_outer_ordering() {
        assert!(super::PAUSE_ACK_TIMEOUT < super::STOP_RESPONSE_TIMEOUT);
        assert!(super::STOP_RESPONSE_TIMEOUT < super::WORKER_SHUTDOWN_TIMEOUT);
        assert!(super::INITIAL_CALLBACK_TIMEOUT < super::MICROPHONE_READY_TIMEOUT);
        assert!(super::ESTABLISHED_CALLBACK_TIMEOUT < super::INITIAL_CALLBACK_TIMEOUT);
    }

    #[test]
    fn shutdown_is_processed_without_audio_samples() {
        let (_sample_tx, sample_rx) = RingBuffer::<f32>::new(48_000);
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            run_consumer(
                48_000,
                None,
                sample_rx,
                cmd_rx,
                None,
                None,
                None,
                None,
                Arc::new(CaptureTransportState::default()),
                Arc::new(AtomicBool::new(false)),
                Instant::now(),
            );
            let _ = done_tx.send(());
        });

        cmd_tx.send(Cmd::Shutdown).expect("send shutdown");
        let stopped = done_rx.recv_timeout(Duration::from_secs(1));
        worker.join().expect("join consumer");
        assert!(stopped.is_ok(), "shutdown waited for an audio sample");
    }

    #[test]
    fn callback_writes_mono_samples_without_allocation_transport() {
        let (mut producer, mut consumer) = RingBuffer::<f32>::new(8);
        let transport = CaptureTransportState::default();

        AudioRecorder::write_input_to_ring(
            &[0.25f32, -0.5, 1.0],
            1,
            None,
            &mut producer,
            &transport,
        );

        let mut output = [0.0; 3];
        consumer.pop_entire_slice(&mut output).expect("samples");
        assert_eq!(output, [0.25, -0.5, 1.0]);
        assert_eq!(transport.callback_heartbeat.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn callback_downmixes_or_selects_multichannel_input() {
        let transport = CaptureTransportState::default();
        let (mut average_tx, mut average_rx) = RingBuffer::<f32>::new(4);
        AudioRecorder::write_input_to_ring(
            &[1.0f32, 3.0, -1.0, 1.0],
            2,
            None,
            &mut average_tx,
            &transport,
        );
        let mut averaged = [0.0; 2];
        average_rx
            .pop_entire_slice(&mut averaged)
            .expect("averaged samples");
        assert_eq!(averaged, [2.0, 0.0]);

        let (mut selected_tx, mut selected_rx) = RingBuffer::<f32>::new(4);
        AudioRecorder::write_input_to_ring(
            &[1.0f32, 3.0, -1.0, 1.0],
            2,
            Some(1),
            &mut selected_tx,
            &transport,
        );
        let mut selected = [0.0; 2];
        selected_rx
            .pop_entire_slice(&mut selected)
            .expect("selected samples");
        assert_eq!(selected, [3.0, 1.0]);
    }

    #[test]
    fn callback_acknowledges_pause_without_writing() {
        let (mut producer, consumer) = RingBuffer::<f32>::new(4);
        let transport = CaptureTransportState::default();
        transport.pause_requested.store(true, Ordering::Release);

        AudioRecorder::write_input_to_ring(&[1.0f32, 2.0], 1, None, &mut producer, &transport);

        assert_eq!(consumer.slots(), 0);
        assert!(transport.pause_acknowledged.load(Ordering::Acquire));
        assert_eq!(transport.callback_heartbeat.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn callback_partially_fills_ring_and_counts_only_dropped_audio() {
        let (mut producer, mut consumer) = RingBuffer::<f32>::new(2);
        let transport = CaptureTransportState::default();

        AudioRecorder::write_input_to_ring(&[1.0f32, 2.0, 3.0], 1, None, &mut producer, &transport);

        let mut captured = [0.0; 2];
        consumer
            .pop_entire_slice(&mut captured)
            .expect("partial callback audio");
        assert_eq!(captured, [1.0, 2.0]);
        assert_eq!(transport.overrun_samples.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn ring_wraparound_preserves_both_read_slices_in_order() {
        let (mut producer, mut consumer) = RingBuffer::<f32>::new(5);
        let transport = CaptureTransportState::default();
        producer
            .push_entire_slice(&[1.0, 2.0, 3.0, 4.0])
            .expect("initial samples");
        let mut discarded = [0.0; 3];
        consumer
            .pop_entire_slice(&mut discarded)
            .expect("advance ring head");

        AudioRecorder::write_input_to_ring(
            &[5.0f32, 6.0, 7.0, 8.0],
            1,
            None,
            &mut producer,
            &transport,
        );

        let chunk = consumer.read_chunk(5).expect("wrapped samples");
        let (first, second) = chunk.as_slices();
        assert!(!first.is_empty());
        assert!(!second.is_empty());
        let ordered = first
            .iter()
            .chain(second.iter())
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(ordered, [4.0, 5.0, 6.0, 7.0, 8.0]);
    }

    #[test]
    fn active_overrun_warns_once_and_consumer_keeps_recording() {
        let (mut producer, consumer) = RingBuffer::<f32>::new(480);
        let transport = Arc::new(CaptureTransportState::default());
        let stream_error = Arc::new(AtomicBool::new(false));
        let observed_stream_error = Arc::clone(&stream_error);
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let (blocked_tx, blocked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
        let callback_count = Arc::new(AtomicUsize::new(0));
        let (warning_tx, warning_rx) = mpsc::channel();
        let consumer_transport = Arc::clone(&transport);
        let worker = thread::spawn(move || {
            run_consumer(
                16_000,
                None,
                consumer,
                cmd_rx,
                None,
                Some(Arc::new(move |_generation, _frame| {
                    if callback_count.fetch_add(1, Ordering::AcqRel) == 1 {
                        let _ = blocked_tx.send(());
                        let _ = release_rx.lock().unwrap().recv();
                    }
                })),
                None,
                Some(Arc::new(move |generation, dropped_samples| {
                    let _ = warning_tx.send((generation, dropped_samples));
                })),
                consumer_transport,
                stream_error,
                Instant::now(),
            );
        });

        let (ready_tx, ready_rx) = mpsc::channel();
        cmd_tx
            .send(Cmd::Start(
                super::VadPolicy::Disabled,
                Instant::now(),
                7,
                ready_tx,
            ))
            .expect("start");
        AudioRecorder::write_input_to_ring(&vec![0.25f32; 480], 1, None, &mut producer, &transport);
        ready_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("capture ready");
        let ring_available_deadline = Instant::now() + Duration::from_secs(1);
        while producer.slots() < 480 {
            assert!(
                Instant::now() < ring_available_deadline,
                "consumer did not commit the first chunk"
            );
            thread::sleep(Duration::from_millis(1));
        }

        AudioRecorder::write_input_to_ring(&vec![0.5f32; 480], 1, None, &mut producer, &transport);
        blocked_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("consumer blocked in downstream processing");
        AudioRecorder::write_input_to_ring(&vec![0.75f32; 600], 1, None, &mut producer, &transport);
        release_tx.send(()).expect("release consumer");

        let warning = warning_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("nonfatal overrun warning");
        assert_eq!(warning, (7, 600));
        assert!(!observed_stream_error.load(Ordering::Acquire));
        assert!(!worker.is_finished(), "overrun terminated the consumer");

        let (reply_tx, reply_rx) = mpsc::channel();
        cmd_tx.send(Cmd::Stop(reply_tx)).expect("stop");
        let pause_deadline = Instant::now() + Duration::from_secs(1);
        while !transport.pause_requested.load(Ordering::Acquire) {
            assert!(Instant::now() < pause_deadline, "pause was not requested");
            thread::sleep(Duration::from_millis(1));
        }
        AudioRecorder::write_input_to_ring(&[99.0f32], 1, None, &mut producer, &transport);
        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("stop reply");

        assert!(
            warning_rx.try_recv().is_err(),
            "warning emitted more than once"
        );
        cmd_tx.send(Cmd::Shutdown).expect("shutdown");
        worker.join().expect("consumer worker");
    }

    #[test]
    fn stop_acknowledgment_drains_all_samples_before_replying() {
        let (mut producer, consumer) = RingBuffer::<f32>::new(16_000);
        let transport = Arc::new(CaptureTransportState::default());
        let stream_error = Arc::new(AtomicBool::new(false));
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let consumer_transport = Arc::clone(&transport);
        let worker = thread::spawn(move || {
            run_consumer(
                16_000,
                None,
                consumer,
                cmd_rx,
                None,
                None,
                None,
                None,
                consumer_transport,
                stream_error,
                Instant::now(),
            );
        });

        let (ready_tx, ready_rx) = mpsc::channel();
        cmd_tx
            .send(Cmd::Start(
                super::VadPolicy::Disabled,
                Instant::now(),
                1,
                ready_tx,
            ))
            .expect("start");
        AudioRecorder::write_input_to_ring(
            &[0.25f32, -0.5, 1.0],
            1,
            None,
            &mut producer,
            &transport,
        );
        ready_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("capture ready");

        let (reply_tx, reply_rx) = mpsc::channel();
        cmd_tx.send(Cmd::Stop(reply_tx)).expect("stop");
        let pause_deadline = Instant::now() + Duration::from_secs(1);
        while !transport.pause_requested.load(Ordering::Acquire) {
            assert!(Instant::now() < pause_deadline, "pause was not requested");
            thread::sleep(Duration::from_millis(1));
        }
        // Queue shutdown while Stop is still inside its pause wait. The worker
        // must finish Stop (including its reply) before processing Shutdown.
        cmd_tx.send(Cmd::Shutdown).expect("queue shutdown");
        // This invocation represents the first callback after Stop. It must
        // acknowledge the pause without adding samples behind the EOS boundary.
        AudioRecorder::write_input_to_ring(&[99.0f32], 1, None, &mut producer, &transport);

        let samples = reply_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("stop reply");
        assert_eq!(&samples[..3], &[0.25, -0.5, 1.0]);
        assert!(!samples.contains(&99.0));

        worker.join().expect("consumer worker");
    }

    #[test]
    fn stop_then_start_reuses_worker_without_stale_pause_acknowledgment() {
        let (mut producer, consumer) = RingBuffer::<f32>::new(16_000);
        let transport = Arc::new(CaptureTransportState::default());
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let consumer_transport = Arc::clone(&transport);
        let worker = thread::spawn(move || {
            run_consumer(
                16_000,
                None,
                consumer,
                cmd_rx,
                None,
                None,
                None,
                None,
                consumer_transport,
                Arc::new(AtomicBool::new(false)),
                Instant::now(),
            );
        });

        let (ready_tx, ready_rx) = mpsc::channel();
        cmd_tx
            .send(Cmd::Start(
                super::VadPolicy::Disabled,
                Instant::now(),
                1,
                ready_tx,
            ))
            .expect("first start");
        AudioRecorder::write_input_to_ring(&vec![0.25f32; 480], 1, None, &mut producer, &transport);
        ready_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first capture ready");

        let (first_reply_tx, first_reply_rx) = mpsc::channel();
        cmd_tx.send(Cmd::Stop(first_reply_tx)).expect("first stop");
        let first_pause_deadline = Instant::now() + Duration::from_secs(1);
        while !transport.pause_requested.load(Ordering::Acquire) {
            assert!(
                Instant::now() < first_pause_deadline,
                "first pause was not requested"
            );
            thread::sleep(Duration::from_millis(1));
        }
        AudioRecorder::write_input_to_ring(&[99.0f32], 1, None, &mut producer, &transport);
        first_reply_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first stop reply");

        let (ready_tx, ready_rx) = mpsc::channel();
        cmd_tx
            .send(Cmd::Start(
                super::VadPolicy::Disabled,
                Instant::now(),
                2,
                ready_tx,
            ))
            .expect("second start");
        let resume_deadline = Instant::now() + Duration::from_secs(1);
        while transport.pause_requested.load(Ordering::Acquire) {
            assert!(Instant::now() < resume_deadline, "worker did not resume");
            thread::sleep(Duration::from_millis(1));
        }
        AudioRecorder::write_input_to_ring(&vec![0.5f32; 480], 1, None, &mut producer, &transport);
        ready_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second capture ready");

        let (second_reply_tx, second_reply_rx) = mpsc::channel();
        cmd_tx
            .send(Cmd::Stop(second_reply_tx))
            .expect("second stop");
        let second_pause_deadline = Instant::now() + Duration::from_secs(1);
        while !transport.pause_requested.load(Ordering::Acquire) {
            assert!(
                Instant::now() < second_pause_deadline,
                "second pause was not requested"
            );
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            matches!(
                second_reply_rx.recv_timeout(Duration::from_millis(20)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "stale acknowledgment completed the second Stop"
        );
        AudioRecorder::write_input_to_ring(&[99.0f32], 1, None, &mut producer, &transport);
        let second_samples = second_reply_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("second stop reply");
        assert!(second_samples.contains(&0.5));
        assert!(!second_samples.contains(&99.0));

        cmd_tx.send(Cmd::Shutdown).expect("shutdown");
        worker.join().expect("consumer worker");
    }

    #[test]
    fn established_callback_stall_reports_error_and_ends_worker() {
        let (mut producer, consumer) = RingBuffer::<f32>::new(16_000);
        let transport = Arc::new(CaptureTransportState::default());
        let stream_error = Arc::new(AtomicBool::new(false));
        let observed_error = Arc::clone(&stream_error);
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let (error_tx, error_rx) = mpsc::channel();
        let consumer_transport = Arc::clone(&transport);
        let worker = thread::spawn(move || {
            run_consumer(
                16_000,
                None,
                consumer,
                cmd_rx,
                None,
                None,
                Some(Arc::new(move |_generation, detail| {
                    let _ = error_tx.send(detail.to_string());
                })),
                None,
                consumer_transport,
                stream_error,
                Instant::now(),
            );
        });

        let (ready_tx, ready_rx) = mpsc::channel();
        cmd_tx
            .send(Cmd::Start(
                super::VadPolicy::Disabled,
                Instant::now(),
                1,
                ready_tx,
            ))
            .expect("start");
        AudioRecorder::write_input_to_ring(&[0.25f32], 1, None, &mut producer, &transport);
        ready_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("capture ready");

        let detail = error_rx
            .recv_timeout(Duration::from_secs(4))
            .expect("stall error");
        assert!(detail.contains("stopped delivering audio"));
        worker.join().expect("stalled consumer worker");
        assert!(observed_error.load(Ordering::Acquire));
    }

    #[test]
    fn detects_access_is_denied() {
        assert!(is_microphone_access_denied("Access is denied"));
    }

    #[test]
    fn detects_permission_denied() {
        assert!(is_microphone_access_denied("permission denied"));
    }

    #[test]
    fn detects_windows_error_code() {
        assert!(is_microphone_access_denied("WASAPI error: 0x80070005"));
    }

    #[test]
    fn does_not_match_unrelated_errors() {
        assert!(!is_microphone_access_denied("device not found"));
    }

    #[test]
    fn detects_no_input_device() {
        assert!(is_no_input_device_error("No input device found"));
    }

    #[test]
    fn detects_coreaudio_config_error() {
        assert!(is_no_input_device_error(
            "Failed to fetch preferred config: A backend-specific error has occurred: An unknown error unknown to the coreaudio-rs API occurred"
        ));
    }

    #[test]
    fn does_not_match_other_errors_for_no_device() {
        assert!(!is_no_input_device_error("permission denied"));
        assert!(!is_no_input_device_error("device not found"));
    }
}
