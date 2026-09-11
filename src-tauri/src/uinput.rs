use crate::settings::{AutoSubmitKey, PasteMethod};
use evdev::uinput::VirtualDevice;
use evdev::{AttributeSet, EventType, InputEvent, KeyCode};

// The hotkey blocker recognizes this prefix and leaves the paste injector
// alone; otherwise it would grab the injector and hide the forwarded chord
// from the compositor behind a second virtual device.
const VIRTUAL_KEYBOARD_NAME: &str = "handy-keys passthrough: Handy Wayland text input";

pub(crate) fn send_paste_key_combo(paste_method: &PasteMethod) -> Result<(), String> {
    let key_sequence: &[(KeyCode, i32)] = match paste_method {
        PasteMethod::CtrlV => &[
            (KeyCode::KEY_LEFTCTRL, 1),
            (KeyCode::KEY_V, 1),
            (KeyCode::KEY_V, 0),
            (KeyCode::KEY_LEFTCTRL, 0),
        ],
        PasteMethod::CtrlShiftV => &[
            (KeyCode::KEY_LEFTCTRL, 1),
            (KeyCode::KEY_LEFTSHIFT, 1),
            (KeyCode::KEY_V, 1),
            (KeyCode::KEY_V, 0),
            (KeyCode::KEY_LEFTSHIFT, 0),
            (KeyCode::KEY_LEFTCTRL, 0),
        ],
        PasteMethod::ShiftInsert => &[
            (KeyCode::KEY_LEFTSHIFT, 1),
            (KeyCode::KEY_INSERT, 1),
            (KeyCode::KEY_INSERT, 0),
            (KeyCode::KEY_LEFTSHIFT, 0),
        ],
        _ => return Err("Unsupported Wayland paste method".into()),
    };

    emit_key_sequence(key_sequence)
}

pub(crate) fn send_auto_submit_key(auto_submit_key: AutoSubmitKey) -> Result<(), String> {
    let key_sequence: &[(KeyCode, i32)] = match auto_submit_key {
        AutoSubmitKey::Enter => &[(KeyCode::KEY_ENTER, 1), (KeyCode::KEY_ENTER, 0)],
        AutoSubmitKey::CtrlEnter => &[
            (KeyCode::KEY_LEFTCTRL, 1),
            (KeyCode::KEY_ENTER, 1),
            (KeyCode::KEY_ENTER, 0),
            (KeyCode::KEY_LEFTCTRL, 0),
        ],
        AutoSubmitKey::CmdEnter => &[
            (KeyCode::KEY_LEFTMETA, 1),
            (KeyCode::KEY_ENTER, 1),
            (KeyCode::KEY_ENTER, 0),
            (KeyCode::KEY_LEFTMETA, 0),
        ],
    };

    emit_key_sequence(key_sequence)
}

fn emit_key_sequence(key_sequence: &[(KeyCode, i32)]) -> Result<(), String> {
    let mut supported_keys = AttributeSet::<KeyCode>::new();
    for &(key_code, _) in key_sequence {
        supported_keys.insert(key_code);
    }

    let mut virtual_keyboard = VirtualDevice::builder()
        .map_err(|error| format!("Failed to open /dev/uinput: {error}"))?
        .name(VIRTUAL_KEYBOARD_NAME)
        .with_keys(&supported_keys)
        .map_err(|error| format!("Failed to configure the Wayland keyboard: {error}"))?
        .build()
        .map_err(|error| format!("Failed to create the Wayland keyboard: {error}"))?;

    for &(key_code, key_state) in key_sequence {
        virtual_keyboard
            .emit(&[InputEvent::new(
                EventType::KEY.0,
                key_code.code(),
                key_state,
            )])
            .map_err(|error| format!("Failed to emit Wayland keyboard input: {error}"))?;
    }

    Ok(())
}
