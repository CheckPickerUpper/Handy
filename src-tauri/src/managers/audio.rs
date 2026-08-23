use crate::audio_toolkit::{
    list_input_devices,
    vad::{
        SmoothedVad, VAD_OFFLINE_HANGOVER_FRAMES, VAD_ONSET_FRAMES, VAD_PREFILL_FRAMES,
        VAD_STREAMING_HANGOVER_FRAMES,
    },
    AudioRecorder, SileroVad, VadPolicy, MICROPHONE_READY_TIMEOUT,
};
use crate::capture_events::{RecordingErrorEvent, RecordingWarningEvent, MICROPHONE_STALLED_ERROR};
use crate::helpers::clamshell;
use crate::managers::transcription::StreamRouter;
use crate::settings::{get_settings, write_settings, AppSettings};
use crate::utils;
use log::{debug, error, info, trace, warn};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};
use tauri::{Emitter, Manager};

const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const VAD_THRESHOLD: f32 = 0.3;

fn accepts_capture_callback(accepted_generation: &AtomicU64, generation: u64) -> bool {
    accepted_generation.load(Ordering::Acquire) == generation
}

fn claim_capture_generation(
    current_generation: &AtomicU64,
    recording_active: &AtomicBool,
    latch: &AtomicU64,
    generation: u64,
    allow_inactive: bool,
) -> bool {
    if current_generation.load(Ordering::Acquire) != generation
        || (!allow_inactive && !recording_active.load(Ordering::Acquire))
    {
        return false;
    }

    let previous = latch.fetch_max(generation, Ordering::AcqRel);
    previous < generation
        && current_generation.load(Ordering::Acquire) == generation
        && (allow_inactive || recording_active.load(Ordering::Acquire))
}

fn set_mute(mute: bool) {
    // Expected behavior:
    // - Windows: works on most systems using standard audio drivers.
    // - Linux: works on many systems (PipeWire, PulseAudio, ALSA),
    //   but some distros may lack the tools used.
    // - macOS: works on most standard setups via AppleScript.
    // If unsupported, fails silently.

    #[cfg(target_os = "windows")]
    {
        unsafe {
            use windows::Win32::{
                Media::Audio::{
                    eMultimedia, eRender, Endpoints::IAudioEndpointVolume, IMMDeviceEnumerator,
                    MMDeviceEnumerator,
                },
                System::Com::{CoCreateInstance, CoInitializeEx, CLSCTX_ALL, COINIT_MULTITHREADED},
            };

            macro_rules! unwrap_or_return {
                ($expr:expr) => {
                    match $expr {
                        Ok(val) => val,
                        Err(_) => return,
                    }
                };
            }

            // Initialize the COM library for this thread.
            // If already initialized (e.g., by another library like Tauri), this does nothing.
            let _ = CoInitializeEx(None, COINIT_MULTITHREADED);

            let all_devices: IMMDeviceEnumerator =
                unwrap_or_return!(CoCreateInstance(&MMDeviceEnumerator, None, CLSCTX_ALL));
            let default_device =
                unwrap_or_return!(all_devices.GetDefaultAudioEndpoint(eRender, eMultimedia));
            let volume_interface = unwrap_or_return!(
                default_device.Activate::<IAudioEndpointVolume>(CLSCTX_ALL, None)
            );

            let _ = volume_interface.SetMute(mute, std::ptr::null());
        }
    }

    #[cfg(target_os = "linux")]
    {
        use std::process::Command;

        let mute_val = if mute { "1" } else { "0" };
        let amixer_state = if mute { "mute" } else { "unmute" };

        // Try multiple backends to increase compatibility
        // 1. PipeWire (wpctl)
        if Command::new("wpctl")
            .args(["set-mute", "@DEFAULT_AUDIO_SINK@", mute_val])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            return;
        }

        // 2. PulseAudio (pactl)
        if Command::new("pactl")
            .args(["set-sink-mute", "@DEFAULT_SINK@", mute_val])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            return;
        }

        // 3. ALSA (amixer)
        let _ = Command::new("amixer")
            .args(["set", "Master", amixer_state])
            .output();
    }

    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        let script = format!(
            "set volume output muted {}",
            if mute { "true" } else { "false" }
        );
        let _ = Command::new("osascript").args(["-e", &script]).output();
    }
}

/// Reads the current system output mute state, mirroring `set_mute`'s backends.
///
/// Returns `Some(true)`/`Some(false)` when the state could be determined, or
/// `None` when it couldn't (unsupported platform, missing CLI tools, or an
/// error). Callers treat `None` as "unknown" and fall back to unmuting on stop,
/// so we never strand the user's audio muted.
#[cfg(target_os = "windows")]
fn get_mute() -> Option<bool> {
    unsafe {
        use windows::Win32::{
            Media::Audio::{
                eMultimedia, eRender, Endpoints::IAudioEndpointVolume, IMMDeviceEnumerator,
                MMDeviceEnumerator,
            },
            System::Com::{CoCreateInstance, CoInitializeEx, CLSCTX_ALL, COINIT_MULTITHREADED},
        };

        // Matches set_mute: no-op if COM is already initialized on this thread.
        let _ = CoInitializeEx(None, COINIT_MULTITHREADED);

        let all_devices: IMMDeviceEnumerator =
            CoCreateInstance(&MMDeviceEnumerator, None, CLSCTX_ALL).ok()?;
        let default_device = all_devices
            .GetDefaultAudioEndpoint(eRender, eMultimedia)
            .ok()?;
        let volume_interface = default_device
            .Activate::<IAudioEndpointVolume>(CLSCTX_ALL, None)
            .ok()?;

        Some(volume_interface.GetMute().ok()?.as_bool())
    }
}

#[cfg(target_os = "linux")]
fn get_mute() -> Option<bool> {
    use std::process::Command;

    // 1. PipeWire (wpctl): prints "[MUTED]" in the volume line when muted.
    if let Ok(out) = Command::new("wpctl")
        .args(["get-volume", "@DEFAULT_AUDIO_SINK@"])
        .output()
    {
        if out.status.success() {
            return Some(String::from_utf8_lossy(&out.stdout).contains("[MUTED]"));
        }
    }

    // 2. PulseAudio (pactl): prints "Mute: yes" / "Mute: no".
    // Force LC_ALL=C so a localized system still emits the parseable English
    // "yes"/"no" instead of e.g. "ja"/"nein".
    if let Ok(out) = Command::new("pactl")
        .env("LC_ALL", "C")
        .args(["get-sink-mute", "@DEFAULT_SINK@"])
        .output()
    {
        if out.status.success() {
            let s = String::from_utf8_lossy(&out.stdout).to_lowercase();
            if s.contains("yes") {
                return Some(true);
            }
            if s.contains("no") {
                return Some(false);
            }
        }
    }

    // 3. ALSA (amixer): prints "[off]" for muted channels, "[on]" otherwise.
    // LC_ALL=C keeps the "[on]"/"[off]" tokens stable across locales.
    if let Ok(out) = Command::new("amixer")
        .env("LC_ALL", "C")
        .args(["get", "Master"])
        .output()
    {
        if out.status.success() {
            let s = String::from_utf8_lossy(&out.stdout);
            if s.contains("[off]") {
                return Some(true);
            }
            if s.contains("[on]") {
                return Some(false);
            }
        }
    }

    None
}

#[cfg(target_os = "macos")]
fn get_mute() -> Option<bool> {
    use std::process::Command;

    let out = Command::new("osascript")
        .args(["-e", "output muted of (get volume settings)"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    match String::from_utf8_lossy(&out.stdout).trim() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

#[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
fn get_mute() -> Option<bool> {
    None
}

/// Restores the system mute state after our forced mute, given the state
/// captured just before we muted. We only ever need to unmute — and only when
/// the system was NOT already muted beforehand. If the prior state was muted,
/// we leave it muted (the user's own state). If it's unknown (`None`), we
/// default to unmuting so audio is never left stranded muted by us.
fn restore_mute(prev_muted: Option<bool>) {
    if prev_muted != Some(true) {
        set_mute(false);
    }
}

const WHISPER_SAMPLE_RATE: usize = 16000;

/* ──────────────────────────────────────────────────────────────── */

#[derive(Clone, Debug)]
pub enum RecordingState {
    Idle,
    Recording { binding_id: String },
    Stopping,
}

#[derive(Clone, Debug)]
pub enum MicrophoneMode {
    AlwaysOn,
    OnDemand,
}

/// Tracks our forced "mute while recording" so we can restore the user's audio
/// exactly as it was. `did_mute` is true while our mute is active; `prev_muted`
/// is the system mute state captured just before we muted, used to decide
/// whether to unmute on stop (so a system that was already muted stays muted).
#[derive(Debug, Default, Clone, Copy)]
struct MuteState {
    did_mute: bool,
    prev_muted: Option<bool>,
}

/// The persisted microphone preference currently in effect. Clamshell and
/// regular selections are kept distinct so losing a clamshell-only device does
/// not erase the user's normal microphone preference.
enum DesiredMicrophone {
    Default,
    Selected(String),
    Clamshell(String),
}

/// Result of resolving the persisted preference to a live cpal device.
/// `device: None` means cpal should open the system default. The unavailable
/// name is populated only when enumeration succeeded and confirmed that the
/// user's regular selected microphone is missing.
struct MicrophoneResolution {
    device: Option<cpal::Device>,
    unavailable_selected_microphone: Option<String>,
}

/* ──────────────────────────────────────────────────────────────── */

fn create_audio_recorder(
    vad_path: &Path,
    app_handle: &tauri::AppHandle,
    selected_channel: Option<u16>,
    stream_router: Arc<StreamRouter>,
    accepted_callback_generation: Arc<AtomicU64>,
) -> Result<AudioRecorder, anyhow::Error> {
    // A single Silero engine covers both the offline and streaming policies (never
    // active at once within a recording), so the recorder reconfigures its
    // hangover tail per session rather than keeping two ONNX sessions resident.
    let silero = SileroVad::new(vad_path, VAD_THRESHOLD)
        .map_err(|e| anyhow::anyhow!("Failed to create SileroVad: {}", e))?;
    let smoothed_vad = SmoothedVad::new(
        Box::new(silero),
        VAD_PREFILL_FRAMES,
        VAD_OFFLINE_HANGOVER_FRAMES,
        VAD_ONSET_FRAMES,
    );

    // Recorder with VAD, a spectrum-level callback that forwards level updates to
    // the frontend, and an audio-frame callback that feeds live streaming via a
    // shared `StreamRouter` (captured directly, not via Tauri state — see its docs).
    let recorder = AudioRecorder::new()
        .map_err(|e| anyhow::anyhow!("Failed to create AudioRecorder: {}", e))?
        .with_vad(
            Box::new(smoothed_vad),
            VAD_OFFLINE_HANGOVER_FRAMES,
            VAD_STREAMING_HANGOVER_FRAMES,
        )
        .with_selected_channel(selected_channel)
        .with_level_callback({
            let app_handle = app_handle.clone();
            let accepted_generation = Arc::clone(&accepted_callback_generation);
            move |generation, levels| {
                if accepts_capture_callback(&accepted_generation, generation) {
                    utils::emit_levels(&app_handle, &levels);
                }
            }
        })
        .with_audio_callback({
            let router = stream_router;
            let accepted_generation = accepted_callback_generation;
            move |generation, frame| {
                if accepts_capture_callback(&accepted_generation, generation) {
                    router.feed(frame);
                }
            }
        })
        .with_capture_error_callback({
            let app_handle = app_handle.clone();
            move |generation, detail| {
                let manager = app_handle.state::<Arc<AudioRecordingManager>>();
                if accepts_capture_callback(&manager.accepted_callback_generation, generation) {
                    manager.report_capture_failure(
                        generation,
                        "recorder_watchdog",
                        MICROPHONE_STALLED_ERROR,
                        None,
                        detail,
                        false,
                    );
                }
            }
        })
        .with_capture_warning_callback({
            let app_handle = app_handle.clone();
            move |generation, dropped_samples| {
                let manager = app_handle.state::<Arc<AudioRecordingManager>>();
                if accepts_capture_callback(&manager.accepted_callback_generation, generation) {
                    manager.report_capture_warning(generation, dropped_samples);
                }
            }
        });

    Ok(recorder)
}

/* ──────────────────────────────────────────────────────────────── */

/// One recording session's first-sample notification. Waiting on this never
/// blocks the shortcut coordinator: callers hand it to a dedicated worker.
pub enum RecordingReadinessResult {
    Ready,
    Disconnected,
    TimedOut,
}

pub struct RecordingReadiness {
    receiver: mpsc::Receiver<()>,
    generation: u64,
}

impl RecordingReadiness {
    pub fn wait(self) -> RecordingReadinessResult {
        match self.receiver.recv_timeout(MICROPHONE_READY_TIMEOUT) {
            Ok(()) => RecordingReadinessResult::Ready,
            Err(mpsc::RecvTimeoutError::Disconnected) => RecordingReadinessResult::Disconnected,
            Err(mpsc::RecvTimeoutError::Timeout) => RecordingReadinessResult::TimedOut,
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

#[derive(Clone)]
pub struct AudioRecordingManager {
    /// Never assign through this directly — route every write through
    /// `set_state()`, which keeps `recording_active` in sync.
    state: Arc<Mutex<RecordingState>>,
    mode: Arc<Mutex<MicrophoneMode>>,
    app_handle: tauri::AppHandle,

    recorder: Arc<Mutex<Option<AudioRecorder>>>,
    is_open: Arc<Mutex<bool>>,
    is_recording: Arc<Mutex<bool>>,
    mute_state: Arc<Mutex<MuteState>>,
    close_generation: Arc<AtomicU64>,
    cancel_generation: Arc<AtomicU64>,
    stream_router: Arc<StreamRouter>,
    /// Lock-free mirror of "is the state in {Recording, Stopping}",
    /// maintained by `set_state()`. The hot-path `is_recording()` reads THIS
    /// instead of the std `state` mutex, so a UI poll can no longer deadlock
    /// the main/webview thread when a worker holds `state` across a slow
    /// CoreAudio open/close.
    recording_active: Arc<AtomicBool>,
    /// Invalidates asynchronous first-sample UI/chime work when a recording is
    /// stopped or cancelled. This prevents a slow device from producing a late
    /// "ready" indication for a session the user already ended.
    capture_generation: Arc<AtomicU64>,
    /// Generation whose frames and visualization levels may reach downstream
    /// consumers. Cleared after Stop and before detaching a worker so a zombie
    /// cannot feed a newer streaming session.
    accepted_callback_generation: Arc<AtomicU64>,
    /// Generation still allowed to emit the asynchronous first-sample cue.
    /// Set to zero as soon as Stop or cancellation begins without changing the
    /// capture identity used to deduplicate terminal failures.
    readiness_generation: Arc<AtomicU64>,
    /// Highest capture generation for which terminal cleanup was claimed.
    terminal_failure_generation: Arc<AtomicU64>,
    /// Independent latch for the one nonfatal dropped-audio warning. A warning
    /// must never suppress a later terminal failure for the same capture.
    overrun_warning_generation: Arc<AtomicU64>,
    /// Resolution of a *named* microphone (selected or clamshell) to its cpal
    /// device, cached so on-demand recording starts skip the full device
    /// enumeration (~40-110ms). Keyed by the resolved name, so a settings
    /// change misses naturally; cleared when an open fails (device unplugged)
    /// so the retry re-enumerates. The system-default case is never cached —
    /// the recorder resolves the current default itself, cheaply.
    cached_device: Arc<Mutex<Option<(String, cpal::Device)>>>,
}

impl AudioRecordingManager {
    /* ---------- construction ------------------------------------------------ */

    pub fn new(
        app: &tauri::AppHandle,
        stream_router: Arc<StreamRouter>,
    ) -> Result<Self, anyhow::Error> {
        let settings = get_settings(app);
        let mode = if settings.always_on_microphone {
            MicrophoneMode::AlwaysOn
        } else {
            MicrophoneMode::OnDemand
        };

        let manager = Self {
            state: Arc::new(Mutex::new(RecordingState::Idle)),
            mode: Arc::new(Mutex::new(mode.clone())),
            app_handle: app.clone(),

            recorder: Arc::new(Mutex::new(None)),
            is_open: Arc::new(Mutex::new(false)),
            is_recording: Arc::new(Mutex::new(false)),
            mute_state: Arc::new(Mutex::new(MuteState::default())),
            close_generation: Arc::new(AtomicU64::new(0)),
            cancel_generation: Arc::new(AtomicU64::new(0)),
            stream_router,
            recording_active: Arc::new(AtomicBool::new(false)),
            capture_generation: Arc::new(AtomicU64::new(0)),
            accepted_callback_generation: Arc::new(AtomicU64::new(0)),
            readiness_generation: Arc::new(AtomicU64::new(0)),
            terminal_failure_generation: Arc::new(AtomicU64::new(0)),
            overrun_warning_generation: Arc::new(AtomicU64::new(0)),
            cached_device: Arc::new(Mutex::new(None)),
        };

        // Always-on?  Open immediately.
        if matches!(mode, MicrophoneMode::AlwaysOn) {
            manager.start_microphone_stream()?;
        }

        Ok(manager)
    }

    /* ---------- helper methods --------------------------------------------- */

    /// The persisted microphone preference currently in effect. Only runs the
    /// clamshell probe (an `ioreg` subprocess, ~10-20ms) when a clamshell
    /// microphone is actually configured.
    fn desired_microphone(&self, settings: &AppSettings) -> DesiredMicrophone {
        if let Some(clamshell_microphone) = &settings.clamshell_microphone {
            let clamshell_started = Instant::now();
            let is_clamshell = clamshell::is_clamshell().unwrap_or(false);
            debug!(
                "device resolve: clamshell_check={:?} (clamshell={})",
                clamshell_started.elapsed(),
                is_clamshell
            );
            if is_clamshell {
                return DesiredMicrophone::Clamshell(clamshell_microphone.clone());
            }
        }
        match &settings.selected_microphone {
            Some(name) => DesiredMicrophone::Selected(name.clone()),
            None => DesiredMicrophone::Default,
        }
    }

    pub fn invalidate_device_cache(&self) {
        *self.cached_device.lock().unwrap() = None;
    }

    fn claim_generation(&self, latch: &AtomicU64, generation: u64, allow_inactive: bool) -> bool {
        claim_capture_generation(
            &self.capture_generation,
            &self.recording_active,
            latch,
            generation,
            allow_inactive,
        )
    }

    fn report_capture_failure_inner(
        &self,
        generation: u64,
        detector: &str,
        error_type: &str,
        event_detail: Option<String>,
        log_detail: &str,
        allow_inactive: bool,
    ) {
        if !self.claim_generation(
            &self.terminal_failure_generation,
            generation,
            allow_inactive,
        ) {
            debug!(
                "Ignoring duplicate or stale microphone failure from {detector} for capture generation {generation}"
            );
            return;
        }

        error!(
            "Microphone failure claimed by {detector} for capture generation {generation}: {log_detail}"
        );
        let _ = self.app_handle.emit(
            "recording-error",
            RecordingErrorEvent::new(error_type, event_detail),
        );

        // Recorder callbacks run on the consumer thread. Always cancel from a
        // separate worker so cleanup can never wait for or join itself.
        let app = self.app_handle.clone();
        std::thread::spawn(move || utils::cancel_current_operation(&app));
    }

    pub fn report_capture_failure(
        &self,
        generation: u64,
        detector: &str,
        error_type: &str,
        event_detail: Option<String>,
        log_detail: &str,
        allow_inactive: bool,
    ) {
        self.report_capture_failure_inner(
            generation,
            detector,
            error_type,
            event_detail,
            log_detail,
            allow_inactive,
        );
    }

    pub fn report_capture_warning(&self, generation: u64, dropped_samples: u64) {
        if self.terminal_failure_generation.load(Ordering::Acquire) >= generation
            || !self.claim_generation(&self.overrun_warning_generation, generation, false)
            || self.terminal_failure_generation.load(Ordering::Acquire) >= generation
        {
            return;
        }

        debug!(
            "Capture generation {generation} lost microphone samples; showing one nonfatal warning"
        );
        let _ = self.app_handle.emit(
            "recording-warning",
            RecordingWarningEvent::audio_dropped(dropped_samples),
        );
    }

    /// Remove a recorder whose worker might still be alive. Dropping the owner
    /// leaves any detached worker with the old VAD Arc; preload_vad() will build
    /// an independent recorder and VAD for the next keypress.
    fn quarantine_recorder(&self, reason: &str) {
        warn!("Discarding unresponsive microphone recorder: {reason}");
        // Invalidate frame/level forwarding before waiting for teardown. A
        // detached worker that later resumes can no longer reach a live route.
        self.accepted_callback_generation
            .store(0, Ordering::Release);
        // Preserve the manager's global lock order: is_open before recorder.
        let mut open_flag = self.is_open.lock().unwrap();
        let mut old_recorder = self.recorder.lock().unwrap().take();
        if let Some(recorder) = old_recorder.as_mut() {
            let _ = recorder.close();
        }
        *open_flag = false;
        *self.is_recording.lock().unwrap() = false;
        self.invalidate_device_cache();
    }

    fn resolve_microphone_device(&self, settings: &AppSettings) -> MicrophoneResolution {
        let desired = self.desired_microphone(settings);
        let (device_name, selected_microphone) = match desired {
            DesiredMicrophone::Default => {
                debug!("device resolve: no mic configured -> system default");
                return MicrophoneResolution {
                    device: None,
                    unavailable_selected_microphone: None,
                };
            }
            DesiredMicrophone::Selected(name) => (name.clone(), Some(name)),
            DesiredMicrophone::Clamshell(name) => (name, None),
        };

        // Cache hit: skip the full enumeration. A stale device (unplugged)
        // fails at open, where the caller invalidates and retries fresh.
        if let Some((cached_name, device)) = self.cached_device.lock().unwrap().as_ref() {
            if *cached_name == device_name {
                debug!("device resolve: cache hit for '{}'", device_name);
                return MicrophoneResolution {
                    device: Some(device.clone()),
                    unavailable_selected_microphone: None,
                };
            }
        }

        // Only report a selected microphone as unavailable when enumeration
        // itself succeeded. A backend enumeration error may be transient and
        // must not erase the user's persisted preference.
        let enumerate_started = Instant::now();
        let (device, enumeration_succeeded) = match list_input_devices() {
            Ok(devices) => (
                devices
                    .into_iter()
                    .find(|d| d.name == device_name)
                    .map(|d| d.device),
                true,
            ),
            Err(e) => {
                debug!("Failed to list devices, using default: {}", e);
                (None, false)
            }
        };
        debug!(
            "device resolve: enumerate={:?} (found={})",
            enumerate_started.elapsed(),
            device.is_some()
        );
        if let Some(d) = &device {
            *self.cached_device.lock().unwrap() = Some((device_name, d.clone()));
        }

        let unavailable_selected_microphone = if enumeration_succeeded && device.is_none() {
            selected_microphone
        } else {
            None
        };
        MicrophoneResolution {
            device,
            unavailable_selected_microphone,
        }
    }

    /// Keep persisted settings and the UI aligned with a successful runtime
    /// fallback. Re-read first so recovery cannot clear a microphone the user
    /// selected concurrently while the stream was being rebuilt.
    fn persist_default_microphone_after_fallback(&self, unavailable_name: &str) {
        let mut settings = get_settings(&self.app_handle);
        if settings.selected_microphone.as_deref() != Some(unavailable_name) {
            return;
        }

        settings.selected_microphone = None;
        write_settings(&self.app_handle, settings);
        let _ = self.app_handle.emit(
            "settings-changed",
            serde_json::json!({
                "setting": "selected_microphone",
                "value": "Default"
            }),
        );
    }

    fn schedule_lazy_close(&self) {
        let gen = self.close_generation.fetch_add(1, Ordering::SeqCst) + 1;
        let app = self.app_handle.clone();
        std::thread::spawn(move || {
            std::thread::sleep(STREAM_IDLE_TIMEOUT);
            let rm = app.state::<Arc<AudioRecordingManager>>();
            // Hold state lock across the check AND close to serialize against
            // try_start_recording, preventing a race where the stream is closed
            // under an active recording.
            let state = rm.state.lock().unwrap();
            if rm.close_generation.load(Ordering::SeqCst) == gen
                && matches!(*state, RecordingState::Idle)
            {
                // stop_microphone_stream does not acquire the state lock,
                // so holding it here is safe (no deadlock).
                info!(
                    "Closing idle microphone stream after {:?}",
                    STREAM_IDLE_TIMEOUT
                );
                rm.stop_microphone_stream();
            }
        });
    }

    /* ---------- microphone life-cycle -------------------------------------- */

    /// Applies mute if mute_while_recording is enabled and stream is open.
    /// Snapshots the system's prior mute state first so `remove_mute` can
    /// restore it instead of unconditionally unmuting.
    pub fn apply_mute(&self) {
        let settings = get_settings(&self.app_handle);
        if !settings.mute_while_recording {
            return;
        }

        // Lock order: is_open before mute_state (matches stop_microphone_stream).
        let is_open = self.is_open.lock().unwrap();
        let mut mute_guard = self.mute_state.lock().unwrap();
        // Already muted this session — don't re-snapshot, or a duplicate/late
        // apply would overwrite prev_muted with our own forced-muted state and
        // strand audio muted on stop.
        if mute_guard.did_mute {
            return;
        }
        if *is_open {
            mute_guard.prev_muted = get_mute();
            set_mute(true);
            mute_guard.did_mute = true;
            debug!("Mute applied (prev_muted={:?})", mute_guard.prev_muted);
        }
    }

    /// Removes mute if it was applied, restoring the system's prior mute state
    /// (a system already muted before recording stays muted).
    pub fn remove_mute(&self) {
        let mut mute_guard = self.mute_state.lock().unwrap();
        if mute_guard.did_mute {
            restore_mute(mute_guard.prev_muted);
            mute_guard.did_mute = false;
            debug!(
                "Mute removed (restored prev_muted={:?})",
                mute_guard.prev_muted
            );
        }
    }

    pub fn preload_vad(&self) -> Result<(), anyhow::Error> {
        let mut recorder_opt = self.recorder.lock().unwrap();
        if recorder_opt.is_none() {
            let vad_path = self
                .app_handle
                .path()
                .resolve(
                    "resources/models/silero_vad_v4.onnx",
                    tauri::path::BaseDirectory::Resource,
                )
                .map_err(|e| anyhow::anyhow!("Failed to resolve VAD path: {}", e))?;
            let settings = get_settings(&self.app_handle);
            *recorder_opt = Some(create_audio_recorder(
                &vad_path,
                &self.app_handle,
                settings.selected_channel,
                Arc::clone(&self.stream_router),
                Arc::clone(&self.accepted_callback_generation),
            )?);
        }
        Ok(())
    }

    pub fn start_microphone_stream(&self) -> Result<(), anyhow::Error> {
        let mut open_flag = self.is_open.lock().unwrap();
        if *open_flag {
            // `is_open` only records that we opened a stream at some point, not
            // that one is still running. If capture has since failed (mic
            // unplugged mid-session, USB dropout), rebuild it before the next
            // recording instead of handing the caller a stalled recorder.
            let needs_reopen = self
                .recorder
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|rec| rec.needs_reopen());

            if !needs_reopen {
                // trace, not debug: with the aliveness check in
                // try_start_recording this now fires on every keypress in
                // always-on mode.
                trace!("Microphone stream already active");
                return Ok(());
            }

            warn!("Microphone stream is no longer running (device disconnected?); reopening");

            // Torn down inline rather than via stop_microphone_stream(), which
            // takes the `is_open` lock we are already holding.
            {
                let mut mute_guard = self.mute_state.lock().unwrap();
                if mute_guard.did_mute {
                    restore_mute(mute_guard.prev_muted);
                    mute_guard.did_mute = false;
                }
            }
            {
                let mut recorder_guard = self.recorder.lock().unwrap();
                let replace = if let Some(recorder) = recorder_guard.as_mut() {
                    recorder.close().is_err() || recorder.replacement_required()
                } else {
                    false
                };
                if replace {
                    recorder_guard.take();
                }
            }
            *self.is_recording.lock().unwrap() = false;
            *open_flag = false;
            self.invalidate_device_cache();
            // Fall through to the same fresh resolution and fallback path used
            // when an on-demand stream opens after its device was unplugged.
        }

        let start_time = Instant::now();

        // Don't mute immediately - caller will handle muting after audio feedback.
        // The previous stream restored audio on close, so did_mute should already
        // be false here; if it somehow isn't, restore rather than just clearing the
        // flag, which would strand system audio muted.
        {
            let mut mute_guard = self.mute_state.lock().unwrap();
            if mute_guard.did_mute {
                restore_mute(mute_guard.prev_muted);
                mute_guard.did_mute = false;
            }
        }

        // Get the selected device from settings, considering clamshell mode.
        // No pre-flight enumeration here: when nothing is configured the
        // recorder resolves the system default itself, and a machine with no
        // input devices at all fails inside open() with the same
        // "No input device found" error this used to check for.
        let settings = get_settings(&self.app_handle);
        let resolve_started = Instant::now();
        let mut resolution = self.resolve_microphone_device(&settings);
        let resolve_elapsed = resolve_started.elapsed();

        // Ensure VAD is loaded if it wasn't for whatever reason
        let vad_started = Instant::now();
        self.preload_vad()?;
        let vad_elapsed = vad_started.elapsed();

        let open_started = Instant::now();
        let first_open = {
            let mut recorder_guard = self.recorder.lock().unwrap();
            recorder_guard
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("Recorder not available"))?
                .open(resolution.device.clone())
        };
        if let Err(first_err) = first_open {
            let replace = self
                .recorder
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(AudioRecorder::replacement_required);
            if replace {
                // A timed-out backend already consumed the full initialization
                // budget. Detach it now and rebuild lazily on the next keypress
                // rather than freezing cancellation for a second 12-second try.
                let mut old = self.recorder.lock().unwrap().take();
                if let Some(recorder) = old.as_mut() {
                    let _ = recorder.close();
                }
                self.invalidate_device_cache();
                return Err(anyhow::anyhow!("Failed to open recorder: {}", first_err));
            }

            // Ordinary failures can still be a stale cached device or config.
            // Re-resolve and retry those once before surfacing the error.
            warn!("Recorder open failed ({first_err}); re-resolving device and retrying once");
            self.invalidate_device_cache();
            resolution = self.resolve_microphone_device(&settings);

            let second_open = self
                .recorder
                .lock()
                .unwrap()
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("Recorder not available after recovery"))?
                .open(resolution.device.clone());
            if let Err(error) = second_open {
                let replace = self
                    .recorder
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(AudioRecorder::replacement_required);
                if replace {
                    let mut old = self.recorder.lock().unwrap().take();
                    if let Some(recorder) = old.as_mut() {
                        let _ = recorder.close();
                    }
                }
                return Err(anyhow::anyhow!("Failed to open recorder: {}", error));
            }
        }
        debug!(
            "mic stream breakdown: device_resolve={:?} vad_ensure={:?} open={:?}",
            resolve_elapsed,
            vad_elapsed,
            open_started.elapsed()
        );

        *open_flag = true;
        if let Some(unavailable_name) = resolution.unavailable_selected_microphone {
            // Do this only after the default stream opened successfully. A
            // failed fallback must not erase the user's microphone preference.
            self.persist_default_microphone_after_fallback(&unavailable_name);
        }
        // This timing covers through cpal's stream.play() returning — i.e. the
        // point cpal surfaces as "stream running." It does NOT guarantee the
        // host audio device is producing samples yet; the first input callback
        // fires asynchronously one buffer period later (hardware dependent,
        // typically ~10–200ms on macOS, longer on Bluetooth/USB).
        info!(
            "Microphone stream initialized in {:?}",
            start_time.elapsed()
        );
        Ok(())
    }

    pub fn stop_microphone_stream(&self) {
        let mut open_flag = self.is_open.lock().unwrap();
        if !*open_flag {
            return;
        }

        {
            let mut mute_guard = self.mute_state.lock().unwrap();
            if mute_guard.did_mute {
                restore_mute(mute_guard.prev_muted);
            }
            mute_guard.did_mute = false;
        }

        {
            let mut recorder_guard = self.recorder.lock().unwrap();
            let replace = if let Some(recorder) = recorder_guard.as_mut() {
                // If still recording, stop first.
                let stop_failed = if *self.is_recording.lock().unwrap() {
                    let failed = recorder.stop().is_err();
                    *self.is_recording.lock().unwrap() = false;
                    failed
                } else {
                    false
                };
                let close_failed = recorder.close().is_err();
                stop_failed || close_failed || recorder.replacement_required()
            } else {
                false
            };
            if replace {
                recorder_guard.take();
                self.invalidate_device_cache();
            }
        }

        self.accepted_callback_generation
            .store(0, Ordering::Release);
        *open_flag = false;
        debug!("Microphone stream stopped");
    }

    /* ---------- mode switching --------------------------------------------- */

    pub fn update_mode(&self, new_mode: MicrophoneMode) -> Result<(), anyhow::Error> {
        let cur_mode = self.mode.lock().unwrap().clone();

        match (cur_mode, &new_mode) {
            (MicrophoneMode::AlwaysOn, MicrophoneMode::OnDemand) => {
                if matches!(*self.state.lock().unwrap(), RecordingState::Idle) {
                    self.close_generation.fetch_add(1, Ordering::SeqCst);
                    self.stop_microphone_stream();
                }
            }
            (MicrophoneMode::OnDemand, MicrophoneMode::AlwaysOn) => {
                self.close_generation.fetch_add(1, Ordering::SeqCst);
                self.start_microphone_stream()?;
            }
            _ => {}
        }

        *self.mode.lock().unwrap() = new_mode;
        Ok(())
    }

    /* ---------- recording --------------------------------------------------- */

    /// The one place `state` is written. Derives `recording_active` (the
    /// lock-free mirror read by `is_recording()`) from the new value itself,
    /// so the two can never drift: a new `RecordingState` variant only needs
    /// its active-set membership decided here, once.
    fn set_state(&self, guard: &mut RecordingState, new_state: RecordingState) {
        *guard = new_state;
        self.recording_active.store(
            matches!(
                *guard,
                RecordingState::Recording { .. } | RecordingState::Stopping
            ),
            Ordering::SeqCst,
        );
    }

    pub fn try_start_recording(
        &self,
        binding_id: &str,
        vad_policy: VadPolicy,
    ) -> Result<RecordingReadiness, String> {
        let mut state = self.state.lock().unwrap();

        if let RecordingState::Idle = *state {
            let generation = self.capture_generation.fetch_add(1, Ordering::AcqRel) + 1;
            self.readiness_generation
                .store(generation, Ordering::Release);

            // Cancel any pending lazy close (no-op in always-on mode, where
            // closes are never scheduled).
            self.close_generation.fetch_add(1, Ordering::SeqCst);
            // Opens the stream in on-demand mode. In always-on mode the stream
            // is normally already open and this is a cheap aliveness check —
            // but if the capture worker died (device disconnect), it rebuilds
            // the stream instead of leaving every subsequent start wedged on
            // "Recorder not available".
            if let Err(e) = self.start_microphone_stream() {
                let msg = format!("{e}");
                self.accepted_callback_generation
                    .store(0, Ordering::Release);
                self.readiness_generation.store(0, Ordering::Release);
                let error_type = if crate::audio_toolkit::is_microphone_access_denied(&msg) {
                    "microphone_permission_denied"
                } else if crate::audio_toolkit::is_no_input_device_error(&msg) {
                    "no_input_device"
                } else if msg.to_lowercase().contains("timed out") {
                    MICROPHONE_STALLED_ERROR
                } else {
                    "unknown"
                };
                self.report_capture_failure(
                    generation,
                    "stream_initialization",
                    error_type,
                    Some(msg.clone()),
                    &msg,
                    true,
                );
                return Err(msg);
            }

            self.accepted_callback_generation
                .store(generation, Ordering::Release);
            if let Some(rec) = self.recorder.lock().unwrap().as_ref() {
                match rec.start(vad_policy, generation) {
                    Ok(receiver) => {
                        *self.is_recording.lock().unwrap() = true;
                        self.set_state(
                            &mut state,
                            RecordingState::Recording {
                                binding_id: binding_id.to_string(),
                            },
                        );
                        debug!("Recording requested for binding {binding_id}");
                        Ok(RecordingReadiness {
                            receiver,
                            generation,
                        })
                    }
                    Err(error) => {
                        let message = format!("Failed to start recorder: {error}");
                        self.accepted_callback_generation
                            .store(0, Ordering::Release);
                        self.readiness_generation.store(0, Ordering::Release);
                        self.report_capture_failure(
                            generation,
                            "start_command",
                            "unknown",
                            Some(message.clone()),
                            &message,
                            true,
                        );
                        Err(message)
                    }
                }
            } else {
                let message = "Recorder not available".to_string();
                self.accepted_callback_generation
                    .store(0, Ordering::Release);
                self.readiness_generation.store(0, Ordering::Release);
                self.report_capture_failure(
                    generation,
                    "start_command",
                    "unknown",
                    Some(message.clone()),
                    &message,
                    true,
                );
                Err(message)
            }
        } else {
            Err("Already recording".to_string())
        }
    }

    pub fn update_selected_device(&self) -> Result<(), anyhow::Error> {
        // Device settings changed; re-enumerate the device and restart capture.
        self.invalidate_device_cache();
        let was_open = *self.is_open.lock().unwrap();
        if was_open {
            self.close_generation.fetch_add(1, Ordering::SeqCst);
            self.stop_microphone_stream();
            self.start_microphone_stream()?;
        }
        Ok(())
    }

    pub fn update_selected_channel(
        &self,
        selected_channel: Option<u16>,
    ) -> Result<(), anyhow::Error> {
        // Serialize against recording start/stop. Restarting an active capture
        // would discard its samples and leave the manager's recording state out
        // of sync with the new recorder.
        let state = self.state.lock().unwrap();
        if !matches!(*state, RecordingState::Idle) {
            return Err(anyhow::anyhow!(
                "Cannot change the input channel while recording"
            ));
        }

        let previous_channel = get_settings(&self.app_handle).selected_channel;
        let was_open = *self.is_open.lock().unwrap();
        if was_open {
            self.close_generation.fetch_add(1, Ordering::SeqCst);
            self.stop_microphone_stream();
        }
        if let Some(recorder) = self.recorder.lock().unwrap().as_mut() {
            recorder.set_selected_channel(selected_channel);
        }
        if was_open {
            if let Err(error) = self.start_microphone_stream() {
                if let Some(recorder) = self.recorder.lock().unwrap().as_mut() {
                    recorder.set_selected_channel(previous_channel);
                }
                return Err(error);
            }
        }
        drop(state);
        Ok(())
    }

    /// Invalidate pending first-sample UI and audio-feedback work immediately.
    /// Called at the beginning of stop, before the slower capture drain starts.
    pub fn invalidate_recording_readiness(&self) {
        self.readiness_generation.store(0, Ordering::Release);
    }

    pub fn is_recording_readiness_current(&self, generation: u64) -> bool {
        self.readiness_generation.load(Ordering::Acquire) == generation
    }

    pub fn cancel_generation(&self) -> u64 {
        self.cancel_generation.load(Ordering::Acquire)
    }

    pub fn was_cancelled_since(&self, generation: u64) -> bool {
        self.cancel_generation.load(Ordering::Acquire) != generation
    }

    pub fn stop_recording(&self, binding_id: &str, cancel_generation: u64) -> Option<Vec<f32>> {
        let capture_generation = self.capture_generation.load(Ordering::Acquire);
        self.invalidate_recording_readiness();
        let mut state = self.state.lock().unwrap();

        match *state {
            RecordingState::Recording {
                binding_id: ref active,
            } if active == binding_id => {
                self.set_state(&mut state, RecordingState::Stopping);
                drop(state);

                // Optionally keep recording for a bit longer to capture trailing audio.
                // This is only the explicit user setting; streaming VAD must not add
                // hidden post-release capture time.
                let settings = get_settings(&self.app_handle);
                let buffer_ms = settings.extra_recording_buffer_ms;
                if buffer_ms > 0 {
                    debug!(
                        "Extra recording buffer: sleeping {}ms before stopping",
                        buffer_ms
                    );
                    let started = Instant::now();
                    let buffer = Duration::from_millis(buffer_ms);
                    while started.elapsed() < buffer {
                        if self.was_cancelled_since(cancel_generation) {
                            debug!("Recording stop cancelled during extra buffer");
                            break;
                        }
                        let remaining = buffer.saturating_sub(started.elapsed());
                        std::thread::sleep(remaining.min(Duration::from_millis(25)));
                    }
                }

                let stop_result = if let Some(rec) = self.recorder.lock().unwrap().as_ref() {
                    rec.stop()
                } else {
                    Err(Box::new(std::io::Error::other("Recorder not available"))
                        as Box<dyn std::error::Error>)
                };

                self.accepted_callback_generation
                    .store(0, Ordering::Release);
                *self.is_recording.lock().unwrap() = false;
                if let Err(error) = &stop_result {
                    self.report_capture_failure(
                        capture_generation,
                        "stop_response_timeout",
                        MICROPHONE_STALLED_ERROR,
                        None,
                        &error.to_string(),
                        false,
                    );
                    self.quarantine_recorder(&error.to_string());
                }
                self.set_state(&mut self.state.lock().unwrap(), RecordingState::Idle);

                // In on-demand mode, close the mic (lazily if the setting is enabled).
                // An unresponsive recorder was already quarantined above.
                if stop_result.is_ok()
                    && matches!(*self.mode.lock().unwrap(), MicrophoneMode::OnDemand)
                {
                    if get_settings(&self.app_handle).lazy_stream_close {
                        self.schedule_lazy_close();
                    } else {
                        self.stop_microphone_stream();
                    }
                }

                let samples = match stop_result {
                    Ok(samples) => samples,
                    Err(error) => {
                        error!("stop() failed: {error}");
                        return None;
                    }
                };

                if self.was_cancelled_since(cancel_generation) {
                    debug!("Recording stop cancelled; discarding captured samples");
                    return None;
                }

                // Pad if very short
                let s_len = samples.len();
                // debug!("Got {} samples", s_len);
                if s_len < WHISPER_SAMPLE_RATE && s_len > 0 {
                    let mut padded = samples;
                    padded.resize(WHISPER_SAMPLE_RATE * 5 / 4, 0.0);
                    Some(padded)
                } else {
                    Some(samples)
                }
            }
            _ => None,
        }
    }
    pub fn is_recording(&self) -> bool {
        // Lock-free: mirrors the `state` {Recording, Stopping} membership via
        // an atomic maintained by `set_state()`. Polled from the webview/main
        // thread, so it MUST NOT take the `state` mutex (a worker can hold it
        // across a slow CoreAudio open/close → main-thread deadlock / UI
        // freeze).
        self.recording_active.load(Ordering::SeqCst)
    }

    /// Cancel any ongoing recording without returning audio samples
    pub fn cancel_recording(&self) {
        let capture_generation = self.capture_generation.load(Ordering::Acquire);
        self.invalidate_recording_readiness();
        self.cancel_generation.fetch_add(1, Ordering::AcqRel);
        let mut state = self.state.lock().unwrap();

        match *state {
            RecordingState::Recording { .. } => {
                // Keep starts excluded until bounded recorder teardown finishes;
                // publishing Idle early would let a new recorder race with
                // quarantine of the old generation.
                self.set_state(&mut state, RecordingState::Stopping);
                drop(state);

                // Cancellation discards the tail, so invalidate all downstream
                // callbacks before asking the old worker to stop.
                self.accepted_callback_generation
                    .store(0, Ordering::Release);
                let stop_result = self
                    .recorder
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(AudioRecorder::stop);
                *self.is_recording.lock().unwrap() = false;

                let stop_failed = matches!(stop_result, Some(Err(_)));
                if let Some(Err(error)) = stop_result {
                    self.report_capture_failure(
                        capture_generation,
                        "cancel_stop_timeout",
                        MICROPHONE_STALLED_ERROR,
                        None,
                        &error.to_string(),
                        true,
                    );
                    self.quarantine_recorder(&error.to_string());
                }

                // In on-demand mode, close the mic (lazily if the setting is enabled).
                if !stop_failed && matches!(*self.mode.lock().unwrap(), MicrophoneMode::OnDemand) {
                    if get_settings(&self.app_handle).lazy_stream_close {
                        self.schedule_lazy_close();
                    } else {
                        self.stop_microphone_stream();
                    }
                }

                self.set_state(&mut self.state.lock().unwrap(), RecordingState::Idle);
            }
            RecordingState::Stopping => {
                debug!("Cancellation requested while recording is stopping");
            }
            RecordingState::Idle => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{accepts_capture_callback, claim_capture_generation};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    #[test]
    fn stale_worker_frames_and_levels_are_rejected() {
        let accepted = AtomicU64::new(12);

        assert!(accepts_capture_callback(&accepted, 12));
        assert!(!accepts_capture_callback(&accepted, 11));

        // Quarantine clears the accepted generation before detaching.
        accepted.store(0, Ordering::Release);
        assert!(!accepts_capture_callback(&accepted, 12));
    }

    #[test]
    fn capture_failure_claim_is_once_per_generation_and_rejects_stale_workers() {
        let current = AtomicU64::new(4);
        let active = AtomicBool::new(true);
        let terminal_latch = AtomicU64::new(0);

        assert!(claim_capture_generation(
            &current,
            &active,
            &terminal_latch,
            4,
            false
        ));
        assert!(!claim_capture_generation(
            &current,
            &active,
            &terminal_latch,
            4,
            false
        ));

        current.store(5, Ordering::Release);
        assert!(!claim_capture_generation(
            &current,
            &active,
            &terminal_latch,
            4,
            false
        ));
        assert!(claim_capture_generation(
            &current,
            &active,
            &terminal_latch,
            5,
            false
        ));
    }

    #[test]
    fn warning_and_terminal_claims_do_not_suppress_each_other() {
        let current = AtomicU64::new(9);
        let active = AtomicBool::new(true);
        let warning_latch = AtomicU64::new(0);
        let terminal_latch = AtomicU64::new(0);

        assert!(claim_capture_generation(
            &current,
            &active,
            &warning_latch,
            9,
            false
        ));
        assert!(claim_capture_generation(
            &current,
            &active,
            &terminal_latch,
            9,
            false
        ));
    }

    #[test]
    fn inactive_capture_is_claimable_only_for_synchronous_start_cleanup() {
        let current = AtomicU64::new(2);
        let active = AtomicBool::new(false);
        let latch = AtomicU64::new(0);

        assert!(!claim_capture_generation(
            &current, &active, &latch, 2, false
        ));
        assert!(claim_capture_generation(&current, &active, &latch, 2, true));
    }
}
