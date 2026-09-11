/// Returns the appropriate CPAL host for the current platform.
/// On Linux, prefers native PipeWire and falls back to ALSA. On other
/// platforms, uses the default host.
pub fn get_cpal_host() -> cpal::Host {
    #[cfg(target_os = "linux")]
    {
        cpal::host_from_id(cpal::HostId::PipeWire)
            .or_else(|_| cpal::host_from_id(cpal::HostId::Alsa))
            .unwrap_or_else(|_| cpal::default_host())
    }
    #[cfg(not(target_os = "linux"))]
    {
        cpal::default_host()
    }
}
