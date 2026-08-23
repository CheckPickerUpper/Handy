use serde::Serialize;

pub const MICROPHONE_STALLED_ERROR: &str = "microphone_stalled";
pub const AUDIO_DROPPED_WARNING: &str = "audio_dropped";

#[derive(Clone, Debug, Serialize)]
pub struct RecordingErrorEvent {
    pub error_type: String,
    pub detail: Option<String>,
}

impl RecordingErrorEvent {
    pub fn new(error_type: impl Into<String>, detail: Option<String>) -> Self {
        Self {
            error_type: error_type.into(),
            detail,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct RecordingWarningEvent {
    pub warning_type: String,
    pub dropped_samples: u64,
}

impl RecordingWarningEvent {
    pub fn audio_dropped(dropped_samples: u64) -> Self {
        Self {
            warning_type: AUDIO_DROPPED_WARNING.to_string(),
            dropped_samples,
        }
    }
}
