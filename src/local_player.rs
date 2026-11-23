//! Direct local lossless FLAC playback through Spotifast's own audio pipeline.
//!
//! Plays downloaded FLAC files directly via `symphonia` -> `eq::Processor` ->
//! `vis::AudioTap` (post-EQ, pre-volume, 44.1 kHz) -> `resample::Resampler`
//! (only when the output device's sample rate differs from the FLAC's native
//! rate) -> volume gain -> `limiter::Limiter` -> `fastframe-audio`, bypassing
//! librespot and requiring no `spotify:local:` tag matching.
//!
//! All file I/O, decoding, DSP, and audio stream management run off the UI
//! thread on a dedicated worker thread. Stopping playback releases the OS audio
//! stream completely so switching back to librespot or Spotify Connect works
//! cleanly.

use std::collections::VecDeque;
use std::fs::File;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use fastframe_audio::{Buffer, BufferSize, DeviceId, Maintained, OutputOptions, Reason, Render};
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::{CODEC_TYPE_NULL, Decoder, DecoderOptions};
use symphonia::core::errors::Error as SymphoniaError;
use symphonia::core::formats::{FormatOptions, FormatReader, SeekMode, SeekTo};
use symphonia::core::io::MediaSourceStream;
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;
use symphonia::core::units::Time;

use crate::eq::{Processor as EqProcessor, SharedEq};
use crate::limiter::Limiter;
use crate::player::Playback;
use crate::resample::Resampler;
use crate::vis::AudioTap;

/// Channel count of Spotifast's internal DSP and visualizer pipeline.
const STEREO_CHANNELS: usize = 2;

/// Sample rate expected by `vis::AudioTap` (spectrum analyser, scope, MilkDrop).
const VIS_SAMPLE_RATE: u32 = 44_100;

/// Stream name shown in the system volume mixer.
const MEDIA_NAME: &str = "Spotifast local playback";

/// How much audio to buffer ahead of the audio callback.
const QUEUE_AHEAD: Duration = Duration::from_millis(200);

/// Maximum time to wait for queued audio to drain on end-of-track or stop.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Length of the smooth Play/Pause/Stop fade envelope.
const TRANSPORT_FADE: Duration = Duration::from_millis(50);

/// Length of the smooth volume/mute gain ramp on the audio callback thread.
const VOLUME_RAMP: Duration = Duration::from_millis(30);

/// How often position updates are emitted during active playback.
const POSITION_EMIT_INTERVAL: Duration = Duration::from_millis(100);

/// Poll interval while waiting for ring-buffer space or drain.
const WORKER_POLL_INTERVAL: Duration = Duration::from_millis(15);

/// Rewind threshold for `previous()`: past 3 seconds, restart the current track.
const PREVIOUS_RESTART_THRESHOLD_MS: u64 = 3_000;

/// Consecutive corrupt-frame decode errors tolerated before failing playback.
const MAX_CONSECUTIVE_DECODE_ERRORS: usize = 16;

/// Lowest sample rate at which the 10-band EQ (top band 16 kHz) stays strictly
/// below Nyquist. All standard FLAC rates (44.1 to 192 kHz) are well above this.
const MIN_EQ_SAMPLE_RATE: u32 = 32_001;

/// Callback invoked whenever [`LocalFlacState`] changes.
pub type StateHook = Arc<dyn Fn(LocalFlacState) + Send + Sync>;

/// Metadata and file location for a locally stored lossless FLAC track.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LocalFlacTrack {
    pub path: PathBuf,
    pub title: String,
    pub artists: Vec<String>,
    pub album: String,
    pub duration_ms: u64,
    pub playlist_id: Option<String>,
    pub track_id: Option<String>,
    pub quality_label: Option<String>,
    pub sample_rate_hz: Option<u32>,
    pub bit_depth: Option<u8>,
}

/// Snapshot of local FLAC playback state emitted to the UI / application layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalFlacState {
    pub active: bool,
    pub playback: Playback,
    pub track: Option<LocalFlacTrack>,
    pub position_ms: u64,
    pub duration_ms: u64,
    pub volume_percent: u8,
    pub muted: bool,
    pub error: Option<String>,
    pub queue: Vec<LocalFlacTrack>,
    pub queue_index: usize,
}

impl Default for LocalFlacState {
    fn default() -> Self {
        Self {
            active: false,
            playback: Playback::Stopped,
            track: None,
            position_ms: 0,
            duration_ms: 0,
            volume_percent: 100,
            muted: false,
            error: None,
            queue: Vec::new(),
            queue_index: 0,
        }
    }
}

/// Stream format metadata probed from a FLAC file without opening an audio device.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlacInfo {
    pub sample_rate_hz: u32,
    pub channels: u16,
    pub bit_depth: Option<u8>,
    pub duration_ms: u64,
    pub total_frames: Option<u64>,
}

impl FlacInfo {
    /// Human-readable quality label, e.g. `"24-bit / 96 kHz FLAC"` or `"16-bit / 44.1 kHz FLAC"`.
    pub fn quality_label(&self) -> String {
        let khz = if self.sample_rate_hz.is_multiple_of(1_000) {
            format!("{} kHz", self.sample_rate_hz / 1_000)
        } else {
            let tenths = (self.sample_rate_hz + 50) / 100;
            format!("{}.{} kHz", tenths / 10, tenths % 10)
        };
        match self.bit_depth {
            Some(bits) => format!("{bits}-bit / {khz} FLAC"),
            None => format!("{khz} FLAC"),
        }
    }
}

/// Probes a FLAC file's stream parameters via `symphonia` without opening any
/// audio device.
pub fn decode_flac_probe(path: &Path) -> Result<FlacInfo> {
    let (_format, _track_id, info) = open_flac_reader(path)?;
    Ok(info)
}

/// Decodes a FLAC file's full PCM stream into interleaved `f32` samples via
/// `symphonia`, returning `(sample_rate_hz, channels, samples)`.
pub fn decode_flac_samples(path: &Path) -> Result<(u32, u16, Vec<f32>)> {
    decode_flac_samples_from(path, 0)
}

/// Decodes a FLAC file starting at `start_ms` (using `FormatReader::seek` when
/// `start_ms > 0`) into interleaved `f32` samples, returning
/// `(sample_rate_hz, channels, samples)`.
pub fn decode_flac_samples_from(path: &Path, start_ms: u64) -> Result<(u32, u16, Vec<f32>)> {
    let (mut format, track_id, info) = open_flac_reader(path)?;
    let track = format
        .tracks()
        .iter()
        .find(|t| t.id == track_id)
        .ok_or_else(|| anyhow!("FLAC track {track_id} disappeared after probe"))?;
    let mut decoder = symphonia::default::get_codecs()
        .make(&track.codec_params, &DecoderOptions::default())
        .context("failed to create FLAC decoder")?;

    let mut trim_frames = 0u64;
    if start_ms > 0 {
        let seeked = format
            .seek(
                SeekMode::Accurate,
                SeekTo::Time {
                    time: Time::from(Duration::from_millis(start_ms)),
                    track_id: Some(track_id),
                },
            )
            .with_context(|| format!("failed to seek FLAC to {start_ms} ms"))?;
        decoder.reset();
        trim_frames = seeked.required_ts.saturating_sub(seeked.actual_ts);
    }

    let mut out = Vec::new();
    let mut sample_buf: Option<SampleBuffer<f32>> = None;
    let channels = usize::from(info.channels.max(1));

    loop {
        let packet = match format.next_packet() {
            Ok(packet) => packet,
            Err(SymphoniaError::IoError(err)) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(SymphoniaError::ResetRequired) => {
                decoder.reset();
                continue;
            }
            Err(err) => return Err(err).context("failed reading FLAC packet"),
        };
        if packet.track_id() != track_id {
            continue;
        }
        let decoded = match decoder.decode(&packet) {
            Ok(buf) => buf,
            Err(SymphoniaError::DecodeError(msg)) => {
                log::warn!("skipping corrupt FLAC frame in {}: {msg}", path.display());
                continue;
            }
            Err(err) => return Err(err).context("failed decoding FLAC packet"),
        };
        let frames = decoded.frames();
        if frames == 0 {
            continue;
        }
        let spec = *decoded.spec();
        let buf =
            sample_buf.get_or_insert_with(|| SampleBuffer::new(decoded.capacity() as u64, spec));
        if buf.capacity() < decoded.capacity() * spec.channels.count() {
            *buf = SampleBuffer::new(decoded.capacity() as u64, spec);
        }
        buf.copy_interleaved_ref(decoded);
        let slice = buf.samples();

        let skip = (trim_frames as usize).min(frames);
        trim_frames = trim_frames.saturating_sub(skip as u64);
        let start_sample = skip * channels;
        if start_sample < slice.len() {
            out.extend_from_slice(&slice[start_sample..]);
        }
    }

    Ok((info.sample_rate_hz, info.channels, out))
}

fn open_flac_reader(path: &Path) -> Result<(Box<dyn FormatReader>, u32, FlacInfo)> {
    let file =
        File::open(path).with_context(|| format!("failed to open FLAC file {}", path.display()))?;
    let source = MediaSourceStream::new(Box::new(file), Default::default());
    let mut hint = Hint::new();
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        hint.with_extension(ext);
    } else {
        hint.with_extension("flac");
    }
    let probed = symphonia::default::get_probe()
        .format(
            &hint,
            source,
            &FormatOptions {
                enable_gapless: true,
                ..Default::default()
            },
            &MetadataOptions::default(),
        )
        .with_context(|| format!("failed to probe audio file {}", path.display()))?;

    let track = probed
        .format
        .default_track()
        .filter(|t| t.codec_params.codec != CODEC_TYPE_NULL)
        .or_else(|| {
            probed
                .format
                .tracks()
                .iter()
                .find(|t| t.codec_params.codec != CODEC_TYPE_NULL)
        })
        .ok_or_else(|| anyhow!("no decodable audio track in {}", path.display()))?;

    let track_id = track.id;
    let params = &track.codec_params;
    let sample_rate_hz = params
        .sample_rate
        .filter(|&sr| sr > 0)
        .ok_or_else(|| anyhow!("missing sample rate in {}", path.display()))?;
    let channels = params
        .channels
        .map(|c| c.count() as u16)
        .filter(|&c| c > 0)
        .ok_or_else(|| anyhow!("missing channel layout in {}", path.display()))?;
    let bit_depth = params
        .bits_per_sample
        .or(params.bits_per_coded_sample)
        .map(|b| b as u8);
    let total_frames = params.n_frames;
    let duration_ms = total_frames
        .map(|frames| frames_to_ms(frames, sample_rate_hz))
        .unwrap_or(0);

    Ok((
        probed.format,
        track_id,
        FlacInfo {
            sample_rate_hz,
            channels,
            bit_depth,
            duration_ms,
            total_frames,
        },
    ))
}

fn frames_to_ms(frames: u64, sample_rate: u32) -> u64 {
    if sample_rate == 0 {
        return 0;
    }
    (u128::from(frames) * 1_000 / u128::from(sample_rate)) as u64
}

/// The length to report and seek within: the file's own frame count when it
/// has one. Library metadata can run longer than the audio, and a seek past
/// the end would leave the decoder with nothing to play.
fn stream_duration_ms(file_ms: u64, metadata_ms: u64) -> u64 {
    if file_ms > 0 { file_ms } else { metadata_ms }
}

/// Converts a volume percentage (0..=100) to linear gain using Spotifast's
/// cubic volume curve, matching the built-in librespot and Soloist players.
pub fn volume_gain(percent: u8, muted: bool) -> f32 {
    if muted {
        return 0.0;
    }
    (f32::from(percent.min(100)) / 100.0).powi(3)
}

/// Gain for the centre and surround channels folded into front left and right
/// (ITU-R BS.775: -3 dB).
const DOWNMIX_MINUS_3_DB: f64 = std::f64::consts::FRAC_1_SQRT_2;

/// Folds one interleaved multichannel frame into front left and right.
///
/// Channels follow the FLAC order: front left, front right, then centre,
/// LFE and rear pairs depending on the count. The centre and rear pairs go
/// into both outputs at -3 dB; the LFE is dropped. Layouts past 5.1 fold only
/// the first rear pair.
fn fold_frame_to_stereo(frame: &[f64]) -> (f64, f64) {
    let at = |index: usize| frame.get(index).copied().unwrap_or(0.0);
    let (centre, rear) = match frame.len() {
        3 => (Some(2), None),
        4 => (None, Some((2, 3))),
        5 => (Some(2), Some((3, 4))),
        _ => (Some(2), Some((4, 5))),
    };
    let mut left = at(0);
    let mut right = at(1);
    if let Some(index) = centre {
        let folded = at(index) * DOWNMIX_MINUS_3_DB;
        left += folded;
        right += folded;
    }
    if let Some((rear_left, rear_right)) = rear {
        left += at(rear_left) * DOWNMIX_MINUS_3_DB;
        right += at(rear_right) * DOWNMIX_MINUS_3_DB;
    }
    (left, right)
}

/// Converts interleaved `channels`-channel `f64` samples into interleaved stereo
/// `f64` samples, sanitizing any non-finite values.
fn to_interleaved_stereo_f64(interleaved: &[f64], channels: usize, out: &mut Vec<f64>) {
    out.clear();
    let sanitize = |v: f64| if v.is_finite() { v } else { 0.0 };
    match channels {
        0 => {}
        1 => {
            out.reserve(interleaved.len() * 2);
            for &s in interleaved {
                let s = sanitize(s);
                out.push(s);
                out.push(s);
            }
        }
        2 => {
            out.reserve(interleaved.len());
            for &s in interleaved {
                out.push(sanitize(s));
            }
        }
        n => {
            let frames = interleaved.len() / n;
            out.reserve(frames * 2);
            for frame in interleaved.chunks_exact(n) {
                let (left, right) = fold_frame_to_stereo(frame);
                out.push(sanitize(left));
                out.push(sanitize(right));
            }
        }
    }
}

/// Stateful DSP pipeline for one local FLAC stream:
/// `EQ (at native track rate)` -> `AudioTap (post-EQ, pre-volume, 44.1 kHz)` ->
/// `Resampler (only if track_sr != device_sr)`.
///
/// Volume and the output limiter are deliberately NOT applied here so that:
/// 1. Visualizers always receive post-EQ, pre-volume audio ("zero volume still dances").
/// 2. Live volume/mute changes take effect immediately on queued audio with a
///    smooth 30 ms ramp on the output render thread.
/// 3. The limiter sees the signal as it reaches the speakers: it runs after the
///    volume gain in [`LocalRender`], so quiet playback is never compressed.
pub struct AudioPipeline {
    shared_eq: SharedEq,
    eq: EqProcessor,
    tap: Arc<AudioTap>,
    tap_resampler: Option<Resampler>,
    output_resampler: Option<Resampler>,
    track_sr: u32,
    device_sr: u32,
}

impl AudioPipeline {
    /// Creates a pipeline for native `track_sr` feeding a device at `device_sr`.
    pub fn new(shared_eq: SharedEq, tap: Arc<AudioTap>, track_sr: u32, device_sr: u32) -> Self {
        let track_sr = track_sr.max(1);
        let device_sr = device_sr.max(1);
        Self {
            eq: EqProcessor::at_rate(shared_eq.clone(), track_sr),
            shared_eq,
            tap,
            tap_resampler: Resampler::new(track_sr, VIS_SAMPLE_RATE, STEREO_CHANNELS),
            output_resampler: Resampler::new(track_sr, device_sr, STEREO_CHANNELS),
            track_sr,
            device_sr,
        }
    }

    /// Returns whether output resampling is active (`track_sr != device_sr`).
    pub fn is_resampling_output(&self) -> bool {
        self.output_resampler.is_some()
    }

    /// Returns the track's native sample rate.
    pub fn track_sample_rate(&self) -> u32 {
        self.track_sr
    }

    /// Returns the currently targeted output device sample rate.
    pub fn device_sample_rate(&self) -> u32 {
        self.device_sr
    }

    /// Updates the output device sample rate (e.g. after a default device switch).
    pub fn set_device_sample_rate(&mut self, device_sr: u32) {
        let device_sr = device_sr.max(1);
        if self.device_sr != device_sr {
            self.device_sr = device_sr;
            self.output_resampler = Resampler::new(self.track_sr, device_sr, STEREO_CHANNELS);
        }
    }

    /// Discards filter, resampler, and visualizer history (on seek or track change).
    pub fn reset(&mut self) {
        self.eq = EqProcessor::at_rate(self.shared_eq.clone(), self.track_sr);
        self.tap_resampler = Resampler::new(self.track_sr, VIS_SAMPLE_RATE, STEREO_CHANNELS);
        self.output_resampler = Resampler::new(self.track_sr, self.device_sr, STEREO_CHANNELS);
        self.tap.clear();
    }

    /// Processes interleaved stereo `f64` samples at `track_sr` through:
    /// EQ -> `AudioTap` (44.1 kHz, post-EQ, pre-volume) -> optional output
    /// resampler to `device_sr`. The output is still pre-volume and unlimited.
    pub fn process_stereo_f64(&mut self, stereo: &mut [f64]) -> Vec<f32> {
        if stereo.is_empty() || !stereo.len().is_multiple_of(STEREO_CHANNELS) {
            return Vec::new();
        }
        for sample in stereo.iter_mut() {
            if !sample.is_finite() {
                *sample = 0.0;
            }
        }

        // 1. Equalizer at native track rate.
        if self.track_sr >= MIN_EQ_SAMPLE_RATE {
            self.eq.process(stereo);
        }

        // 2. Visualizer tap: post-EQ, pre-volume, resampled to 44.1 kHz only if needed.
        if let Some(tap_resampler) = &mut self.tap_resampler {
            let tap_f32: Vec<f32> = stereo.iter().map(|&v| v as f32).collect();
            let resampled = tap_resampler.process(&tap_f32);
            let tap_f64: Vec<f64> = resampled.into_iter().map(f64::from).collect();
            self.tap.push(&tap_f64, 1.0);
        } else {
            self.tap.push(stereo, 1.0);
        }

        // 3. Convert to f32 and resample to device rate ONLY if track_sr != device_sr.
        //    The limiter runs later, after the volume gain (see `LocalRender`).
        let out_f32: Vec<f32> = stereo.iter().map(|&v| v as f32).collect();
        match &mut self.output_resampler {
            Some(resampler) => resampler.process(&out_f32),
            None => out_f32,
        }
    }
}

/// Fixed-point full-scale gain for the sample-clocked transport fade envelope.
const ENVELOPE_SCALE: u32 = 1 << 24;

fn fade_frames(sample_rate: u32, length: Duration) -> u32 {
    (u64::from(sample_rate.max(1)) * length.as_millis() as u64 / 1_000).max(1) as u32
}

fn step_over(frames: u32) -> u32 {
    ENVELOPE_SCALE.div_ceil(frames.max(1)).max(1)
}

/// Sample-clocked Play/Pause/Stop fade envelope shared between worker and audio thread.
struct Envelope {
    level: AtomicU32,
    target: AtomicU32,
    step: AtomicU32,
    sample_rate: AtomicU32,
}

impl Envelope {
    fn closed(sample_rate: u32) -> Arc<Self> {
        let sr = sample_rate.max(1);
        let full = step_over(fade_frames(sr, TRANSPORT_FADE));
        Arc::new(Self {
            level: AtomicU32::new(0),
            target: AtomicU32::new(0),
            step: AtomicU32::new(full),
            sample_rate: AtomicU32::new(sr),
        })
    }

    fn set_sample_rate(&self, sample_rate: u32) {
        let sr = sample_rate.max(1);
        self.sample_rate.store(sr, Ordering::Relaxed);
        self.step.store(
            step_over(fade_frames(sr, TRANSPORT_FADE)),
            Ordering::Relaxed,
        );
    }

    fn fade_in(&self) {
        let sr = self.sample_rate.load(Ordering::Relaxed);
        self.step.store(
            step_over(fade_frames(sr, TRANSPORT_FADE)),
            Ordering::Relaxed,
        );
        self.target.store(ENVELOPE_SCALE, Ordering::Relaxed);
    }

    fn fade_out(&self, available_frames: u64) {
        let sr = self.sample_rate.load(Ordering::Relaxed);
        let nominal = fade_frames(sr, TRANSPORT_FADE);
        let frames = available_frames.clamp(1, u64::from(nominal)) as u32;
        self.step.store(step_over(frames), Ordering::Relaxed);
        self.target.store(0, Ordering::Relaxed);
    }

    fn close(&self) {
        self.target.store(0, Ordering::Relaxed);
        self.level.store(0, Ordering::Relaxed);
    }

    fn silent(&self) -> bool {
        self.level.load(Ordering::Relaxed) == 0
    }

    fn next_gain(&self) -> f32 {
        let level = self.level.load(Ordering::Relaxed);
        let target = self.target.load(Ordering::Relaxed);
        let step = self.step.load(Ordering::Relaxed);
        let next = match level.cmp(&target) {
            std::cmp::Ordering::Less => level.saturating_add(step).min(target),
            std::cmp::Ordering::Greater => level.saturating_sub(step).max(target),
            std::cmp::Ordering::Equal => level,
        };
        self.level.store(next, Ordering::Relaxed);
        level as f32 / ENVELOPE_SCALE as f32
    }
}

/// Linear 30 ms gain ramp for smooth volume/mute changes on the audio thread.
#[derive(Default)]
struct VolumeRamp {
    current: f32,
    target: f32,
    remaining: u32,
    sample_rate: u32,
}

impl VolumeRamp {
    fn next_gain(&mut self, target: f32, sample_rate: u32) -> f32 {
        if target != self.target || sample_rate != self.sample_rate {
            self.target = target;
            self.sample_rate = sample_rate;
            self.remaining = fade_frames(sample_rate, VOLUME_RAMP);
        }
        let gain = self.current;
        if self.remaining > 0 {
            self.current += (self.target - self.current) / self.remaining as f32;
            self.remaining -= 1;
            if self.remaining == 0 {
                self.current = self.target;
            }
        }
        gain
    }
}

/// Shared stereo sample queue between the decoder worker thread and the
/// `fastframe_audio::Render` audio callback.
struct SampleQueue {
    samples: Mutex<VecDeque<f32>>,
    queued_frames: AtomicU64,
    sample_rate: AtomicU32,
    channels: AtomicU16,
    /// Set whenever `Render::configure` switches the device sample rate so the
    /// worker can rebuild its output resampler.
    format_changed: AtomicBool,
    /// Set by `clear` so the render thread discards the limiter's look-ahead
    /// (audio from before the clear) before it plays anything new.
    limiter_flush: AtomicBool,
}

impl SampleQueue {
    fn new(initial_sample_rate: u32) -> Arc<Self> {
        let capacity = (initial_sample_rate as usize) * STEREO_CHANNELS / 2;
        Arc::new(Self {
            samples: Mutex::new(VecDeque::with_capacity(capacity.max(4096))),
            queued_frames: AtomicU64::new(0),
            sample_rate: AtomicU32::new(initial_sample_rate.max(1)),
            channels: AtomicU16::new(STEREO_CHANNELS as u16),
            format_changed: AtomicBool::new(false),
            limiter_flush: AtomicBool::new(false),
        })
    }

    fn push_stereo(&self, stereo: &[f32]) {
        if stereo.is_empty() {
            return;
        }
        let usable = stereo.len() - (stereo.len() % STEREO_CHANNELS);
        let mut queue = self.samples.lock().unwrap_or_else(PoisonError::into_inner);
        queue.extend(&stereo[..usable]);
        self.queued_frames
            .store((queue.len() / STEREO_CHANNELS) as u64, Ordering::Relaxed);
    }

    fn clear(&self) {
        let mut queue = self.samples.lock().unwrap_or_else(PoisonError::into_inner);
        queue.clear();
        self.queued_frames.store(0, Ordering::Relaxed);
        // Under the lock, so the render thread sees it with the emptied queue.
        self.limiter_flush.store(true, Ordering::Release);
    }

    fn queued_frames(&self) -> u64 {
        self.queued_frames.load(Ordering::Relaxed)
    }

    fn device_sample_rate(&self) -> u32 {
        self.sample_rate.load(Ordering::Relaxed)
    }

    fn take_format_changed(&self) -> bool {
        self.format_changed.swap(false, Ordering::AcqRel)
    }
}

/// Runs one stereo frame through the output limiter at full scale.
fn limit_frame(limiter: &mut Limiter, left: f32, right: f32) -> (f32, f32) {
    let mut pair = [f64::from(left), f64::from(right)];
    limiter.process(&mut pair, 1.0);
    (pair[0] as f32, pair[1] as f32)
}

/// Empties the limiter's look-ahead of audio from before a clear: silence goes
/// in and what comes out is discarded. The look-ahead is about 8 ms, so 10 ms
/// of silence is always enough.
fn flush_limiter(limiter: &mut Limiter, sample_rate: u32) {
    for _ in 0..sample_rate.max(1) / 100 + 1 {
        let _ = limit_frame(limiter, 0.0, 0.0);
    }
}

/// `fastframe_audio::Render` implementation pulling stereo samples from
/// [`SampleQueue`], then applying the post-tap volume ramp and transport fade,
/// then the output limiter.
///
/// The limiter comes last so that it guards the level that reaches the device.
/// A quiet track at a low volume is not compressed by a ceiling that the
/// volume has already brought far below full scale, and no volume setting can
/// push the output past full scale.
struct LocalRender {
    queue: Arc<SampleQueue>,
    volume_gain: Arc<AtomicU32>,
    transport: Arc<Envelope>,
    ramp: VolumeRamp,
    /// Look-ahead limiter at the device rate, created whenever the rate changes.
    limiter: Limiter,
    sample_rate: u32,
    channels: u16,
}

impl LocalRender {
    fn new(queue: Arc<SampleQueue>, volume_gain: Arc<AtomicU32>, transport: Arc<Envelope>) -> Self {
        let initial_gain = f32::from_bits(volume_gain.load(Ordering::Relaxed));
        let sr = queue.device_sample_rate();
        Self {
            queue,
            volume_gain,
            transport,
            ramp: VolumeRamp {
                current: initial_gain,
                target: initial_gain,
                remaining: 0,
                sample_rate: sr,
            },
            limiter: Limiter::new(f64::from(sr)),
            sample_rate: sr,
            channels: STEREO_CHANNELS as u16,
        }
    }
}

impl Render for LocalRender {
    fn configure(&mut self, sample_rate: u32, channels: u16) {
        let sr = sample_rate.max(1);
        let ch = channels.max(1);
        let rate_changed = self.sample_rate != sr;
        self.sample_rate = sr;
        self.channels = ch;
        self.queue.sample_rate.store(sr, Ordering::Relaxed);
        self.queue.channels.store(ch, Ordering::Relaxed);
        self.transport.set_sample_rate(sr);
        if rate_changed {
            // The look-ahead is counted in frames, so it must follow the rate.
            self.limiter = Limiter::new(f64::from(sr));
            self.queue.clear();
            self.queue.format_changed.store(true, Ordering::Release);
        }
    }

    fn render(&mut self, out: &mut [f32]) {
        let ch = usize::from(self.channels).max(1);
        let sr = self.sample_rate.max(1);
        let target_vol = f32::from_bits(self.volume_gain.load(Ordering::Relaxed));

        let Ok(mut guard) = self.queue.samples.try_lock() else {
            // Avoid blocking the realtime audio callback if the worker briefly
            // holds the lock; advance ramps and the limiter over silence instead.
            for frame in out.chunks_mut(ch) {
                let _ = self.ramp.next_gain(target_vol, sr);
                let _ = self.transport.next_gain();
                let _ = limit_frame(&mut self.limiter, 0.0, 0.0);
                frame.fill(0.0);
            }
            return;
        };

        if self.queue.limiter_flush.swap(false, Ordering::AcqRel) {
            flush_limiter(&mut self.limiter, sr);
        }

        for frame in out.chunks_mut(ch) {
            let gain = self.ramp.next_gain(target_vol, sr) * self.transport.next_gain();
            let (left, right) = if guard.len() >= STEREO_CHANNELS {
                let left = guard.pop_front().unwrap_or(0.0);
                let right = guard.pop_front().unwrap_or(0.0);
                (left * gain, right * gain)
            } else {
                (0.0, 0.0)
            };
            let (left, right) = limit_frame(&mut self.limiter, left, right);
            match ch {
                1 => frame[0] = 0.5 * (left + right),
                2 => {
                    frame[0] = left;
                    frame[1] = right;
                }
                _ => {
                    frame[0] = left;
                    frame[1] = right;
                    frame[2..].fill(0.0);
                }
            }
        }
        self.queue
            .queued_frames
            .store((guard.len() / STEREO_CHANNELS) as u64, Ordering::Relaxed);
    }
}

/// Abstract audio output stream so the worker thread can run against either
/// `fastframe_audio::Output` in production or an in-memory sink in unit tests.
trait AudioOutput: Send {
    fn sample_rate(&self) -> u32;
    fn push_stereo(&mut self, samples: &[f32]);
    fn queued_frames(&self) -> u64;
    fn clear(&mut self);
    fn pause(&mut self);
    fn resume(&mut self);
    fn fade_in(&self);
    fn fade_out_and_wait(&mut self);
    fn maintain(&mut self) -> Result<bool>;
    fn failed(&self) -> bool;
}

type OutputOpener = Arc<
    dyn Fn(Option<&str>, u32, u32, Arc<AtomicU32>) -> Result<Box<dyn AudioOutput>> + Send + Sync,
>;

struct FastframeOutput {
    device: fastframe_audio::Output<LocalRender>,
    queue: Arc<SampleQueue>,
    transport: Arc<Envelope>,
    buffer_ms: u32,
}

impl AudioOutput for FastframeOutput {
    fn sample_rate(&self) -> u32 {
        self.queue.device_sample_rate()
    }

    fn push_stereo(&mut self, samples: &[f32]) {
        self.queue.push_stereo(samples);
    }

    fn queued_frames(&self) -> u64 {
        self.queue.queued_frames()
    }

    fn clear(&mut self) {
        self.queue.clear();
    }

    fn pause(&mut self) {
        self.device.pause();
        self.transport.close();
    }

    fn resume(&mut self) {
        self.device.resume();
        self.transport.fade_in();
    }

    fn fade_in(&self) {
        self.transport.fade_in();
    }

    fn fade_out_and_wait(&mut self) {
        let frames = self.queue.queued_frames();
        if frames == 0 {
            self.transport.close();
            return;
        }
        self.transport.fade_out(frames);
        let wait =
            Duration::from_millis(u64::from(self.buffer_ms)).saturating_add(TRANSPORT_FADE * 2);
        let deadline = Instant::now() + wait.min(Duration::from_millis(250));
        while !self.transport.silent() && !self.device.failed() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(2));
        }
        self.transport.close();
    }

    fn maintain(&mut self) -> Result<bool> {
        for error in self.device.take_errors() {
            if error.is_fatal() {
                log::error!("local FLAC audio stream error: {error}");
            } else {
                log::warn!("local FLAC audio stream error: {error}");
            }
        }
        match self.device.maintain() {
            Maintained::Reopened {
                device,
                sample_rate,
                reason,
                ..
            } => match reason {
                Reason::Stalled => {
                    log::warn!("local FLAC output stalled; reopened {device} at {sample_rate} Hz")
                }
                Reason::Failed | Reason::DefaultChanged | Reason::Resumed => log::info!(
                    "local FLAC output reopened ({reason:?}): {device} at {sample_rate} Hz"
                ),
            },
            Maintained::Failed(err) => {
                return Err(anyhow!("audio output failed: {err}"));
            }
            Maintained::Unchanged | Maintained::Released => {}
        }
        Ok(self.queue.take_format_changed())
    }

    fn failed(&self) -> bool {
        self.device.failed()
    }
}

fn resolve_audio_device(preferred: Option<&str>) -> fastframe_audio::Device {
    let Some(saved) = preferred.map(str::trim).filter(|s| !s.is_empty()) else {
        return fastframe_audio::Device::Default;
    };
    let resolved = match fastframe_audio::output_devices() {
        Ok(devices) => {
            let found = crate::sink::Saved::among(Some(saved), &devices);
            if found == crate::sink::Saved::Default {
                log::info!(
                    "the saved audio output {saved:?} is not here; playing local FLAC through the default"
                );
            }
            found
        }
        Err(error) => {
            log::warn!("couldn't list audio outputs: {error}");
            crate::sink::Saved::Id(DeviceId::new(saved))
        }
    };
    match resolved {
        crate::sink::Saved::Default => fastframe_audio::Device::Default,
        crate::sink::Saved::Id(id) => fastframe_audio::Device::Id(id),
        crate::sink::Saved::Named { name, .. } => fastframe_audio::Device::Named(name),
    }
}

fn output_options(
    device: fastframe_audio::Device,
    sample_rate: u32,
    buffer_ms: u32,
) -> OutputOptions {
    let clamped = buffer_ms.clamp(
        *crate::sink::BUFFER_MS_RANGE.start(),
        *crate::sink::BUFFER_MS_RANGE.end(),
    );
    OutputOptions {
        device,
        channels: STEREO_CHANNELS as u16,
        sample_rate: Some(sample_rate),
        buffer: Buffer::FixedOnWindows(BufferSize::Duration(Duration::from_millis(u64::from(
            clamped,
        )))),
        follow_default: true,
        media_name: Some(MEDIA_NAME.to_owned()),
        ..OutputOptions::default()
    }
}

fn open_fastframe_output(
    preferred: Option<&str>,
    sample_rate: u32,
    buffer_ms: u32,
    volume_gain: Arc<AtomicU32>,
) -> Result<Box<dyn AudioOutput>> {
    fastframe_audio::raise_feeder_priority();
    let clamped_buffer = buffer_ms.clamp(
        *crate::sink::BUFFER_MS_RANGE.start(),
        *crate::sink::BUFFER_MS_RANGE.end(),
    );
    let queue = SampleQueue::new(sample_rate);
    let transport = Envelope::closed(sample_rate);
    let render = LocalRender::new(Arc::clone(&queue), volume_gain, Arc::clone(&transport));
    let device_choice = resolve_audio_device(preferred);
    let options = output_options(device_choice, sample_rate, clamped_buffer);
    let device = fastframe_audio::Output::open(options, render).map_err(|err| match err {
        fastframe_audio::OpenError::NoDevice => anyhow!("{}", crate::sink::NO_DEVICE),
        other => anyhow!("{other}"),
    })?;
    log::info!(
        "local FLAC output: {} through {} at {} Hz (requested {} Hz)",
        device.device_name(),
        fastframe_audio::host(),
        queue.device_sample_rate(),
        sample_rate,
    );
    // Discard the initial configure notification so the caller reads the opened
    // sample rate directly on creation.
    let _ = queue.take_format_changed();
    Ok(Box::new(FastframeOutput {
        device,
        queue,
        transport,
        buffer_ms: clamped_buffer,
    }))
}

enum Command {
    PlayTrack {
        track: Box<LocalFlacTrack>,
        queue: Vec<LocalFlacTrack>,
        start_index: usize,
    },
    PlayPause,
    Pause,
    Resume,
    Stop,
    Seek(u64),
    Next,
    Previous,
    SetVolume(u8),
    SetMuted(bool),
    SetAudioDevice {
        device: Option<String>,
        buffer_ms: u32,
    },
    Shutdown,
}

/// Thread-safe, non-blocking controller for local lossless FLAC playback.
///
/// Designed to be owned by `Worker` in `src/backend.rs`. Every control method
/// returns immediately without blocking the UI thread; decoding, DSP, and audio
/// output happen on a dedicated background thread.
pub struct LocalFlacPlayer {
    cmd_tx: Mutex<mpsc::Sender<Command>>,
    volume_gain: Arc<AtomicU32>,
    volume_percent: Arc<AtomicU32>,
    muted: Arc<AtomicBool>,
    shared_state: Arc<Mutex<LocalFlacState>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
}

impl LocalFlacPlayer {
    /// Creates a new local FLAC player controller backed by `fastframe-audio`.
    pub fn new(
        eq: SharedEq,
        tap: Arc<AudioTap>,
        audio_device: Option<String>,
        buffer_ms: u32,
        on_state: StateHook,
    ) -> Self {
        Self::with_opener(
            eq,
            tap,
            audio_device,
            buffer_ms,
            on_state,
            Arc::new(open_fastframe_output),
        )
    }

    fn with_opener(
        eq: SharedEq,
        tap: Arc<AudioTap>,
        audio_device: Option<String>,
        buffer_ms: u32,
        on_state: StateHook,
        opener: OutputOpener,
    ) -> Self {
        let initial_state = LocalFlacState::default();
        let initial_gain = volume_gain(initial_state.volume_percent, initial_state.muted);
        let volume_gain_atomic = Arc::new(AtomicU32::new(initial_gain.to_bits()));
        let volume_percent = Arc::new(AtomicU32::new(u32::from(initial_state.volume_percent)));
        let muted = Arc::new(AtomicBool::new(initial_state.muted));
        let shared_state = Arc::new(Mutex::new(initial_state));

        let (cmd_tx, cmd_rx) = mpsc::channel();
        let worker_gain = Arc::clone(&volume_gain_atomic);
        let worker_state = Arc::clone(&shared_state);

        let handle = thread::Builder::new()
            .name("local-flac-player".into())
            .spawn(move || {
                let mut runner = PlayerWorker::new(
                    eq,
                    tap,
                    audio_device,
                    buffer_ms,
                    worker_gain,
                    worker_state,
                    on_state,
                    opener,
                );
                runner.run(cmd_rx);
            })
            .expect("failed to spawn local-flac-player thread");

        Self {
            cmd_tx: Mutex::new(cmd_tx),
            volume_gain: volume_gain_atomic,
            volume_percent,
            muted,
            shared_state,
            worker: Mutex::new(Some(handle)),
        }
    }

    /// Returns the latest playback state snapshot.
    pub fn state(&self) -> LocalFlacState {
        self.shared_state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Starts playing `track` as item `start_index` of `queue`.
    pub fn play_track(
        &self,
        track: LocalFlacTrack,
        queue: Vec<LocalFlacTrack>,
        start_index: usize,
    ) {
        self.send(Command::PlayTrack {
            track: Box::new(track),
            queue,
            start_index,
        });
    }

    /// Toggles between playing and paused (or restarts the current track if stopped).
    pub fn play_pause(&self) {
        self.send(Command::PlayPause);
    }

    /// Pauses playback and suspends the OS audio output stream.
    pub fn pause(&self) {
        self.send(Command::Pause);
    }

    /// Resumes paused playback.
    pub fn resume(&self) {
        self.send(Command::Resume);
    }

    /// Stops playback and releases the OS audio output stream completely.
    pub fn stop(&self) {
        self.send(Command::Stop);
    }

    /// Seeks within the currently loaded track to `position_ms`.
    pub fn seek(&self, position_ms: u64) {
        self.send(Command::Seek(position_ms));
    }

    /// Advances to the next track in the local queue, or stops at the end.
    pub fn next(&self) {
        self.send(Command::Next);
    }

    /// Rewinds to the start of the current track (past 3 s) or steps to the previous track.
    pub fn previous(&self) {
        self.send(Command::Previous);
    }

    /// Sets playback volume (0..=100). Applied post-tap on the output thread.
    pub fn set_volume(&self, percent: u8) {
        let clamped = percent.min(100);
        self.volume_percent
            .store(u32::from(clamped), Ordering::Relaxed);
        let is_muted = self.muted.load(Ordering::Relaxed);
        let gain = volume_gain(clamped, is_muted);
        self.volume_gain.store(gain.to_bits(), Ordering::Relaxed);
        self.send(Command::SetVolume(clamped));
    }

    /// Sets mute state. Applied post-tap on the output thread so visualizers stay active.
    pub fn set_muted(&self, muted: bool) {
        self.muted.store(muted, Ordering::Relaxed);
        let percent = self.volume_percent.load(Ordering::Relaxed) as u8;
        let gain = volume_gain(percent, muted);
        self.volume_gain.store(gain.to_bits(), Ordering::Relaxed);
        self.send(Command::SetMuted(muted));
    }

    /// Updates the preferred audio output device and buffer duration in milliseconds.
    pub fn set_audio_device(&self, device: Option<String>, buffer_ms: u32) {
        self.send(Command::SetAudioDevice { device, buffer_ms });
    }

    fn send(&self, cmd: Command) {
        let _ = self
            .cmd_tx
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .send(cmd);
    }
}

impl Drop for LocalFlacPlayer {
    fn drop(&mut self) {
        self.send(Command::Shutdown);
        if let Some(handle) = self
            .worker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            let _ = handle.join();
        }
    }
}

/// Active decoded FLAC stream owned exclusively by the worker thread.
struct ActiveStream {
    format: Box<dyn FormatReader>,
    decoder: Box<dyn Decoder>,
    track_id: u32,
    info: FlacInfo,
    pipeline: AudioPipeline,
    sample_buf: Option<SampleBuffer<f64>>,
    stereo_buf: Vec<f64>,
    decoded_frames: u64,
    trim_frames: u64,
    eof: bool,
    eof_since: Option<Instant>,
    consecutive_errors: usize,
}

struct PlayerWorker {
    eq: SharedEq,
    tap: Arc<AudioTap>,
    audio_device: Option<String>,
    buffer_ms: u32,
    volume_gain: Arc<AtomicU32>,
    shared_state: Arc<Mutex<LocalFlacState>>,
    on_state: StateHook,
    opener: OutputOpener,
    state: LocalFlacState,
    output: Option<Box<dyn AudioOutput>>,
    output_requested_sr: Option<u32>,
    stream: Option<ActiveStream>,
    last_position_emit: Instant,
    shutdown: bool,
}

impl PlayerWorker {
    #[allow(clippy::too_many_arguments)]
    fn new(
        eq: SharedEq,
        tap: Arc<AudioTap>,
        audio_device: Option<String>,
        buffer_ms: u32,
        volume_gain: Arc<AtomicU32>,
        shared_state: Arc<Mutex<LocalFlacState>>,
        on_state: StateHook,
        opener: OutputOpener,
    ) -> Self {
        let clamped_buffer = buffer_ms.clamp(
            *crate::sink::BUFFER_MS_RANGE.start(),
            *crate::sink::BUFFER_MS_RANGE.end(),
        );
        Self {
            eq,
            tap,
            audio_device,
            buffer_ms: clamped_buffer,
            volume_gain,
            shared_state,
            on_state,
            opener,
            state: LocalFlacState::default(),
            output: None,
            output_requested_sr: None,
            stream: None,
            last_position_emit: Instant::now(),
            shutdown: false,
        }
    }

    fn emit_state(&mut self) {
        let snapshot = self.state.clone();
        *self
            .shared_state
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = snapshot.clone();
        (self.on_state)(snapshot);
    }

    fn run(&mut self, cmd_rx: mpsc::Receiver<Command>) {
        while !self.shutdown {
            if self.state.playback != Playback::Playing || self.stream.is_none() {
                match cmd_rx.recv() {
                    Ok(cmd) => self.handle_command(cmd),
                    Err(_) => break,
                }
                continue;
            }

            // Drain any waiting commands first so controls stay responsive.
            while let Ok(cmd) = cmd_rx.try_recv() {
                self.handle_command(cmd);
                if self.shutdown || self.state.playback != Playback::Playing {
                    break;
                }
            }
            if self.shutdown || self.state.playback != Playback::Playing {
                continue;
            }

            let needs_wait = self.step_playback();
            if needs_wait && !self.shutdown && self.state.playback == Playback::Playing {
                match cmd_rx.recv_timeout(WORKER_POLL_INTERVAL) {
                    Ok(cmd) => self.handle_command(cmd),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        }

        // Release audio output cleanly on shutdown.
        self.release_output(false);
        self.stream = None;
        self.tap.clear();
    }

    fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::PlayTrack {
                track,
                mut queue,
                start_index,
            } => {
                let track = *track;
                let index = if queue.is_empty() {
                    queue.push(track.clone());
                    0
                } else {
                    let idx = start_index.min(queue.len() - 1);
                    if queue[idx].path != track.path {
                        queue[idx] = track.clone();
                    }
                    idx
                };
                self.state.queue = queue;
                self.state.queue_index = index;
                self.start_track(track);
            }
            Command::PlayPause => match self.state.playback {
                Playback::Playing => self.pause_playback(),
                Playback::Paused => self.resume_playback(),
                Playback::Stopped => {
                    if let Some(track) = self
                        .state
                        .track
                        .clone()
                        .or_else(|| self.state.queue.get(self.state.queue_index).cloned())
                    {
                        self.start_track(track);
                    }
                }
                Playback::Loading => {}
            },
            Command::Pause => self.pause_playback(),
            Command::Resume => self.resume_playback(),
            Command::Stop => self.stop_playback(None),
            Command::Seek(position_ms) => self.seek_to(position_ms),
            Command::Next => self.advance_next(),
            Command::Previous => self.step_previous(),
            Command::SetVolume(percent) => {
                let clamped = percent.min(100);
                let gain = volume_gain(clamped, self.state.muted);
                self.volume_gain.store(gain.to_bits(), Ordering::Relaxed);
                self.state.volume_percent = clamped;
                self.emit_state();
            }
            Command::SetMuted(muted) => {
                let gain = volume_gain(self.state.volume_percent, muted);
                self.volume_gain.store(gain.to_bits(), Ordering::Relaxed);
                self.state.muted = muted;
                self.emit_state();
            }
            Command::SetAudioDevice { device, buffer_ms } => {
                let clamped = buffer_ms.clamp(
                    *crate::sink::BUFFER_MS_RANGE.start(),
                    *crate::sink::BUFFER_MS_RANGE.end(),
                );
                let changed = self.audio_device != device || self.buffer_ms != clamped;
                self.audio_device = device;
                self.buffer_ms = clamped;
                if changed && self.output.is_some() {
                    self.reopen_output_for_current_stream();
                }
            }
            Command::Shutdown => {
                self.shutdown = true;
            }
        }
    }

    fn start_track(&mut self, mut track: LocalFlacTrack) {
        self.state.active = true;
        self.state.playback = Playback::Loading;
        self.state.error = None;
        self.state.position_ms = 0;
        self.state.duration_ms = track.duration_ms;
        self.state.track = Some(track.clone());
        self.emit_state();

        let (format, track_id, info) = match open_flac_reader(&track.path) {
            Ok(opened) => opened,
            Err(err) => {
                log::error!("local FLAC open failed: {err:#}");
                self.stop_playback(Some(err.to_string()));
                return;
            }
        };

        let decoder = {
            let Some(sym_track) = format.tracks().iter().find(|t| t.id == track_id) else {
                self.stop_playback(Some("FLAC track metadata missing".into()));
                return;
            };
            match symphonia::default::get_codecs()
                .make(&sym_track.codec_params, &DecoderOptions::default())
            {
                Ok(decoder) => decoder,
                Err(err) => {
                    log::error!("local FLAC decoder init failed: {err}");
                    self.stop_playback(Some(format!("failed to decode FLAC: {err}")));
                    return;
                }
            }
        };

        if track.duration_ms == 0 {
            track.duration_ms = info.duration_ms;
        }
        if track.sample_rate_hz.is_none() {
            track.sample_rate_hz = Some(info.sample_rate_hz);
        }
        if track.bit_depth.is_none() {
            track.bit_depth = info.bit_depth;
        }
        if track.quality_label.is_none() {
            track.quality_label = Some(info.quality_label());
        }
        if let Some(slot) = self.state.queue.get_mut(self.state.queue_index) {
            *slot = track.clone();
        }

        // Open (or reuse when requesting the same native sample rate) the audio output.
        let device_sr = match self.ensure_output(info.sample_rate_hz) {
            Ok(sr) => sr,
            Err(err) => {
                log::error!("local FLAC audio output open failed: {err:#}");
                self.stop_playback(Some(err.to_string()));
                return;
            }
        };

        if let Some(output) = &mut self.output {
            output.clear();
            output.resume();
        }
        self.tap.clear();

        let pipeline = AudioPipeline::new(
            self.eq.clone(),
            Arc::clone(&self.tap),
            info.sample_rate_hz,
            device_sr,
        );

        self.stream = Some(ActiveStream {
            format,
            decoder,
            track_id,
            info,
            pipeline,
            sample_buf: None,
            stereo_buf: Vec::new(),
            decoded_frames: 0,
            trim_frames: 0,
            eof: false,
            eof_since: None,
            consecutive_errors: 0,
        });

        self.state.active = true;
        self.state.playback = Playback::Playing;
        self.state.track = Some(track.clone());
        self.state.duration_ms = stream_duration_ms(info.duration_ms, track.duration_ms);
        self.state.position_ms = 0;
        self.state.error = None;
        self.last_position_emit = Instant::now();
        self.emit_state();
    }

    fn ensure_output(&mut self, requested_sr: u32) -> Result<u32> {
        let needs_reopen = self.output.is_none()
            || self.output_requested_sr != Some(requested_sr)
            || self.output.as_ref().is_some_and(|out| out.failed());
        if needs_reopen {
            self.release_output(true);
            let opened = (self.opener)(
                self.audio_device.as_deref(),
                requested_sr,
                self.buffer_ms,
                Arc::clone(&self.volume_gain),
            )?;
            let sr = opened.sample_rate();
            self.output = Some(opened);
            self.output_requested_sr = Some(requested_sr);
            Ok(sr)
        } else {
            Ok(self
                .output
                .as_ref()
                .map_or(requested_sr, |out| out.sample_rate()))
        }
    }

    fn reopen_output_for_current_stream(&mut self) {
        let Some(track_sr) = self.stream.as_ref().map(|s| s.info.sample_rate_hz) else {
            self.release_output(false);
            return;
        };
        // Where the ear is, before the old device's queue is dropped. The decoder
        // is ahead of that by whatever was queued, and that audio is never heard.
        let heard_ms = self.current_position_ms();
        let was_playing = self.state.playback == Playback::Playing;
        self.release_output(was_playing);
        match (self.opener)(
            self.audio_device.as_deref(),
            track_sr,
            self.buffer_ms,
            Arc::clone(&self.volume_gain),
        ) {
            Ok(mut opened) => {
                let device_sr = opened.sample_rate();
                if let Some(stream) = &mut self.stream {
                    stream.pipeline.set_device_sample_rate(device_sr);
                }
                if was_playing {
                    opened.resume();
                } else {
                    opened.pause();
                }
                self.output = Some(opened);
                self.output_requested_sr = Some(track_sr);
                // Rewind the decoder to the heard position so the new device
                // continues from there instead of skipping the dropped audio.
                self.seek_to(heard_ms);
            }
            Err(err) => {
                log::error!("failed to switch local FLAC audio device: {err:#}");
                self.stop_playback(Some(err.to_string()));
            }
        }
    }

    fn release_output(&mut self, fade: bool) {
        if let Some(mut output) = self.output.take() {
            if fade {
                output.fade_out_and_wait();
            }
            output.clear();
            output.pause();
            drop(output);
        }
        self.output_requested_sr = None;
    }

    fn pause_playback(&mut self) {
        if self.state.playback != Playback::Playing {
            return;
        }
        if let Some(output) = &mut self.output {
            output.fade_out_and_wait();
            output.pause();
        }
        self.tap.clear();
        self.state.position_ms = self.current_position_ms();
        self.state.playback = Playback::Paused;
        self.emit_state();
    }

    fn resume_playback(&mut self) {
        if self.state.playback != Playback::Paused || self.stream.is_none() {
            return;
        }
        if let Some(track_sr) = self.stream.as_ref().map(|s| s.info.sample_rate_hz) {
            match self.ensure_output(track_sr) {
                Ok(device_sr) => {
                    if let Some(stream) = &mut self.stream {
                        stream.pipeline.set_device_sample_rate(device_sr);
                    }
                    if let Some(output) = &mut self.output {
                        output.resume();
                    }
                }
                Err(err) => {
                    log::error!("failed to resume local FLAC output: {err:#}");
                    self.stop_playback(Some(err.to_string()));
                    return;
                }
            }
        }
        self.state.playback = Playback::Playing;
        self.last_position_emit = Instant::now();
        self.emit_state();
    }

    fn stop_playback(&mut self, error: Option<String>) {
        let fade = self.state.playback == Playback::Playing;
        self.release_output(fade);
        self.stream = None;
        self.tap.clear();
        self.state.active = false;
        self.state.playback = Playback::Stopped;
        self.state.position_ms = 0;
        self.state.error = error;
        self.emit_state();
    }

    fn seek_to(&mut self, position_ms: u64) {
        let Some(stream) = &mut self.stream else {
            return;
        };
        // Never seek past the audio that is actually in the file.
        let duration_ms = stream_duration_ms(stream.info.duration_ms, self.state.duration_ms);
        let clamped_ms = if duration_ms > 0 {
            position_ms.min(duration_ms)
        } else {
            position_ms
        };

        let seeked = match stream.format.seek(
            SeekMode::Accurate,
            SeekTo::Time {
                time: Time::from(Duration::from_millis(clamped_ms)),
                track_id: Some(stream.track_id),
            },
        ) {
            Ok(seeked) => seeked,
            Err(err) => {
                log::warn!("local FLAC seek to {clamped_ms} ms failed: {err}");
                return;
            }
        };

        stream.decoder.reset();
        stream.pipeline.reset();
        stream.decoded_frames = seeked.required_ts;
        stream.trim_frames = seeked.required_ts.saturating_sub(seeked.actual_ts);
        stream.eof = false;
        stream.eof_since = None;
        stream.consecutive_errors = 0;

        if let Some(output) = &mut self.output {
            output.clear();
            if self.state.playback == Playback::Playing {
                output.fade_in();
            }
        }

        self.state.position_ms = frames_to_ms(seeked.required_ts, stream.info.sample_rate_hz);
        self.last_position_emit = Instant::now();
        self.emit_state();
    }

    fn advance_next(&mut self) {
        if self.state.queue_index + 1 < self.state.queue.len() {
            self.state.queue_index += 1;
            let next = self.state.queue[self.state.queue_index].clone();
            self.start_track(next);
        } else {
            self.stop_playback(None);
        }
    }

    fn step_previous(&mut self) {
        let pos = self.current_position_ms();
        if self.stream.is_some() && pos > PREVIOUS_RESTART_THRESHOLD_MS {
            self.seek_to(0);
            return;
        }
        if self.state.queue_index > 0 {
            self.state.queue_index -= 1;
            let prev = self.state.queue[self.state.queue_index].clone();
            self.start_track(prev);
        } else if self.stream.is_some() {
            self.seek_to(0);
        }
    }

    fn current_position_ms(&self) -> u64 {
        let Some(stream) = &self.stream else {
            return self.state.position_ms;
        };
        let track_sr = stream.info.sample_rate_hz;
        let queued_device_frames = self.output.as_ref().map_or(0, |out| out.queued_frames());
        let device_sr = stream.pipeline.device_sample_rate().max(1);
        let queued_track_frames = (u128::from(queued_device_frames) * u128::from(track_sr)
            / u128::from(device_sr)) as u64;
        let heard_frames = stream.decoded_frames.saturating_sub(queued_track_frames);
        let ms = frames_to_ms(heard_frames, track_sr);
        if self.state.duration_ms > 0 {
            ms.min(self.state.duration_ms)
        } else {
            ms
        }
    }

    /// Runs one decode/maintain cycle. Returns `true` if the worker should wait
    /// briefly before stepping again (queue full or draining).
    fn step_playback(&mut self) -> bool {
        // Maintain the audio output and follow any sample rate change.
        if let Some(output) = &mut self.output {
            match output.maintain() {
                Ok(format_changed) => {
                    if format_changed {
                        let new_sr = output.sample_rate();
                        if let Some(stream) = &mut self.stream {
                            stream.pipeline.set_device_sample_rate(new_sr);
                        }
                    }
                }
                Err(err) => {
                    log::error!("local FLAC output error: {err:#}");
                    self.stop_playback(Some(err.to_string()));
                    return false;
                }
            }
            if output.failed() {
                self.stop_playback(Some("The audio output stopped working".into()));
                return false;
            }
        } else {
            return true;
        }

        self.maybe_emit_position();

        let (device_sr, queued_frames) = match &self.output {
            Some(out) => (out.sample_rate().max(1), out.queued_frames()),
            None => return true,
        };
        let target_frames = u64::from(device_sr) * QUEUE_AHEAD.as_millis() as u64 / 1_000;

        let eof = self.stream.as_ref().is_none_or(|s| s.eof);
        if eof {
            let timed_out = self
                .stream
                .as_ref()
                .and_then(|s| s.eof_since)
                .is_some_and(|since| since.elapsed() >= DRAIN_TIMEOUT);
            if queued_frames == 0 || timed_out {
                self.advance_next();
                return false;
            }
            return true;
        }

        if queued_frames >= target_frames {
            return true;
        }

        // Decode one packet and push processed samples into the audio output.
        enum PacketResult {
            Samples(Vec<f32>),
            Skip,
            Eof,
            Error(String),
        }

        let result = {
            let Some(stream) = &mut self.stream else {
                return true;
            };
            match stream.format.next_packet() {
                Ok(packet) => {
                    if packet.track_id() != stream.track_id {
                        PacketResult::Skip
                    } else {
                        match stream.decoder.decode(&packet) {
                            Ok(decoded) => {
                                stream.consecutive_errors = 0;
                                let frames = decoded.frames();
                                if frames == 0 {
                                    PacketResult::Skip
                                } else {
                                    let spec = *decoded.spec();
                                    let channels = spec.channels.count().max(1);
                                    let buf = stream.sample_buf.get_or_insert_with(|| {
                                        SampleBuffer::new(decoded.capacity() as u64, spec)
                                    });
                                    if buf.capacity() < decoded.capacity() * channels {
                                        *buf = SampleBuffer::new(decoded.capacity() as u64, spec);
                                    }
                                    buf.copy_interleaved_ref(decoded);

                                    let skip = (stream.trim_frames as usize).min(frames);
                                    stream.trim_frames =
                                        stream.trim_frames.saturating_sub(skip as u64);
                                    let kept_frames = frames - skip;
                                    if kept_frames == 0 {
                                        PacketResult::Skip
                                    } else {
                                        let slice = &buf.samples()[skip * channels..];
                                        to_interleaved_stereo_f64(
                                            slice,
                                            channels,
                                            &mut stream.stereo_buf,
                                        );
                                        let out = stream
                                            .pipeline
                                            .process_stereo_f64(&mut stream.stereo_buf);
                                        stream.decoded_frames = stream
                                            .decoded_frames
                                            .saturating_add(kept_frames as u64);
                                        PacketResult::Samples(out)
                                    }
                                }
                            }
                            Err(SymphoniaError::DecodeError(msg)) => {
                                stream.consecutive_errors += 1;
                                log::warn!(
                                    "local FLAC decode error ({}): {msg}",
                                    stream.consecutive_errors
                                );
                                if stream.consecutive_errors >= MAX_CONSECUTIVE_DECODE_ERRORS {
                                    PacketResult::Error(format!("FLAC decode failed: {msg}"))
                                } else {
                                    PacketResult::Skip
                                }
                            }
                            Err(SymphoniaError::ResetRequired) => {
                                stream.decoder.reset();
                                PacketResult::Skip
                            }
                            Err(err) => PacketResult::Error(format!("FLAC decode error: {err}")),
                        }
                    }
                }
                Err(SymphoniaError::IoError(err)) if err.kind() == ErrorKind::UnexpectedEof => {
                    stream.eof = true;
                    stream.eof_since = Some(Instant::now());
                    PacketResult::Eof
                }
                Err(SymphoniaError::ResetRequired) => {
                    stream.decoder.reset();
                    PacketResult::Skip
                }
                Err(err) => PacketResult::Error(format!("FLAC read error: {err}")),
            }
        };

        match result {
            PacketResult::Samples(samples) => {
                if let Some(output) = &mut self.output {
                    output.push_stereo(&samples);
                }
                false
            }
            PacketResult::Skip => false,
            PacketResult::Eof => {
                self.push_end_of_track_tail();
                true
            }
            PacketResult::Error(message) => {
                self.stop_playback(Some(message));
                false
            }
        }
    }

    /// The output limiter holds about 8 ms of audio to look ahead, so the last
    /// decoded samples only reach the speakers if more audio follows them. Push
    /// 10 ms of silence after the final sample so that tail is played out.
    fn push_end_of_track_tail(&mut self) {
        let Some(output) = &mut self.output else {
            return;
        };
        let frames = output.sample_rate().max(1) as usize / 100 + 1;
        output.push_stereo(&vec![0.0f32; frames * STEREO_CHANNELS]);
    }

    fn maybe_emit_position(&mut self) {
        if self.last_position_emit.elapsed() < POSITION_EMIT_INTERVAL {
            return;
        }
        let pos = self.current_position_ms();
        self.last_position_emit = Instant::now();
        if pos != self.state.position_ms {
            self.state.position_ms = pos;
            self.emit_state();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::f64::consts::TAU;
    use std::sync::atomic::AtomicUsize;

    /// Compile-time check that `LocalFlacPlayer` is `Send + Sync`.
    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn local_flac_player_is_send_and_sync() {
        assert_send_sync::<LocalFlacPlayer>();
    }

    // -------------------------------------------------------------------------
    // Minimal RFC 9639 VERBATIM FLAC synthesizer for deterministic unit tests
    // -------------------------------------------------------------------------

    fn crc8(data: &[u8]) -> u8 {
        let mut crc = 0u8;
        for &byte in data {
            crc ^= byte;
            for _ in 0..8 {
                if crc & 0x80 != 0 {
                    crc = (crc << 1) ^ 0x07;
                } else {
                    crc <<= 1;
                }
            }
        }
        crc
    }

    fn crc16(data: &[u8]) -> u16 {
        let mut crc = 0u16;
        for &byte in data {
            crc ^= u16::from(byte) << 8;
            for _ in 0..8 {
                if crc & 0x8000 != 0 {
                    crc = (crc << 1) ^ 0x8005;
                } else {
                    crc <<= 1;
                }
            }
        }
        crc
    }

    fn encode_utf8_frame_number(num: u32, out: &mut Vec<u8>) {
        if num < 0x80 {
            out.push(num as u8);
        } else if num < 0x800 {
            out.push(0xC0 | ((num >> 6) as u8));
            out.push(0x80 | ((num & 0x3F) as u8));
        } else {
            out.push(0xE0 | ((num >> 12) as u8));
            out.push(0x80 | (((num >> 6) & 0x3F) as u8));
            out.push(0x80 | ((num & 0x3F) as u8));
        }
    }

    /// Synthesizes a valid PCM FLAC file using VERBATIM subframes.
    ///
    /// Supports 16-bit and 24-bit signed samples, mono or stereo, and any
    /// standard sample rate (44.1 kHz, 48 kHz, 88.2 kHz, 96 kHz, 192 kHz).
    /// `interleaved_pcm` must contain `total_frames * channels` signed integer
    /// samples scaled to the target bit depth.
    fn build_verbatim_flac(
        sample_rate: u32,
        channels: u16,
        bit_depth: u8,
        block_size: usize,
        interleaved_pcm: &[i32],
    ) -> Vec<u8> {
        assert!(channels == 1 || channels == 2);
        assert!(bit_depth == 16 || bit_depth == 24);
        assert!(block_size == 512 || block_size == 1024);
        let ch = usize::from(channels);
        assert_eq!(interleaved_pcm.len() % (block_size * ch), 0);
        let total_frames = (interleaved_pcm.len() / ch) as u64;
        let num_blocks = total_frames as usize / block_size;

        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"fLaC");

        // STREAMINFO metadata block (last block = 0x80, length = 34).
        bytes.push(0x80);
        bytes.extend_from_slice(&[0x00, 0x00, 34]);
        bytes.extend_from_slice(&(block_size as u16).to_be_bytes());
        bytes.extend_from_slice(&(block_size as u16).to_be_bytes());
        bytes.extend_from_slice(&[0x00, 0x00, 0x00]); // min frame size unknown
        bytes.extend_from_slice(&[0x00, 0x00, 0x00]); // max frame size unknown
        let packed: u64 = (u64::from(sample_rate) << 44)
            | (u64::from(channels - 1) << 41)
            | (u64::from(bit_depth - 1) << 36)
            | (total_frames & 0x000F_FFFF_FFFF);
        bytes.extend_from_slice(&packed.to_be_bytes());
        bytes.extend_from_slice(&[0u8; 16]); // MD5 unset

        let block_code: u8 = if block_size == 512 { 0b1001 } else { 0b1010 };
        let ch_code: u8 = if channels == 1 { 0b0000 } else { 0b0001 };

        for block_idx in 0..num_blocks {
            let frame_start = bytes.len();
            // Fixed-blocksize sync code.
            bytes.push(0xFF);
            bytes.push(0xF8);
            // Block size code | sample rate from STREAMINFO (0x0).
            bytes.push(block_code << 4);
            // Channel assignment | sample size from STREAMINFO (0b000) | reserved 0.
            bytes.push(ch_code << 4);
            encode_utf8_frame_number(block_idx as u32, &mut bytes);
            let hdr_crc = crc8(&bytes[frame_start..]);
            bytes.push(hdr_crc);

            let base_frame = block_idx * block_size;
            for c in 0..ch {
                // Subframe header: 0 | 000001 (VERBATIM) | 0 (no wasted bits) = 0x02.
                bytes.push(0x02);
                for f in 0..block_size {
                    let sample = interleaved_pcm[(base_frame + f) * ch + c];
                    if bit_depth == 16 {
                        bytes.extend_from_slice(&(sample as i16).to_be_bytes());
                    } else {
                        let be = sample.to_be_bytes();
                        bytes.extend_from_slice(&be[1..4]);
                    }
                }
            }

            let frame_crc = crc16(&bytes[frame_start..]);
            bytes.extend_from_slice(&frame_crc.to_be_bytes());
        }

        bytes
    }

    fn sine_pcm(
        sample_rate: u32,
        channels: u16,
        bit_depth: u8,
        frames: usize,
        hz: f64,
        amplitude: f64,
    ) -> Vec<i32> {
        let max_val = if bit_depth == 16 {
            32_767.0
        } else {
            8_388_607.0
        };
        let mut out = Vec::with_capacity(frames * usize::from(channels));
        for i in 0..frames {
            let t = i as f64 / f64::from(sample_rate);
            let s = (amplitude * (TAU * hz * t).sin() * max_val).round() as i32;
            for _ in 0..channels {
                out.push(s);
            }
        }
        out
    }

    fn temp_dir(name: &str) -> PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "spotifast-local-flac-{name}-{}-{id}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_test_flac(
        dir: &Path,
        filename: &str,
        sample_rate: u32,
        channels: u16,
        bit_depth: u8,
        frames: usize,
        hz: f64,
    ) -> PathBuf {
        let pcm = sine_pcm(sample_rate, channels, bit_depth, frames, hz, 0.25);
        let bytes = build_verbatim_flac(sample_rate, channels, bit_depth, 512, &pcm);
        let path = dir.join(filename);
        std::fs::write(&path, bytes).unwrap();
        path
    }

    // -------------------------------------------------------------------------
    // Mock audio output backend for headless controller tests
    // -------------------------------------------------------------------------

    #[derive(Default)]
    struct MockAudioLog {
        opened_requests: Vec<(Option<String>, u32, u32)>,
        active_outputs: usize,
        rendered_samples: Vec<f32>,
        pause_count: usize,
        resume_count: usize,
        drop_count: usize,
    }

    /// Frames queued in the mock device. Like a real device it plays them out at
    /// the device rate while it is running, and holds them while it is paused.
    struct MockQueue {
        frames: f64,
        clock: Instant,
    }

    struct MockOutput {
        device_sr: u32,
        volume_gain: Arc<AtomicU32>,
        log: Arc<Mutex<MockAudioLog>>,
        paused: bool,
        queue: Mutex<MockQueue>,
    }

    impl MockOutput {
        /// Plays out what the device has consumed since the last call.
        fn drain(&self) {
            let mut queue = self.queue.lock().unwrap();
            let now = Instant::now();
            if !self.paused {
                let elapsed = now.duration_since(queue.clock).as_secs_f64();
                queue.frames = (queue.frames - elapsed * f64::from(self.device_sr)).max(0.0);
            }
            queue.clock = now;
        }
    }

    impl AudioOutput for MockOutput {
        fn sample_rate(&self) -> u32 {
            self.device_sr
        }

        fn push_stereo(&mut self, samples: &[f32]) {
            self.drain();
            let gain = f32::from_bits(self.volume_gain.load(Ordering::Relaxed));
            self.queue.lock().unwrap().frames += (samples.len() / STEREO_CHANNELS) as f64;
            let mut log = self.log.lock().unwrap();
            log.rendered_samples
                .extend(samples.iter().map(|&s| s * gain));
        }

        fn queued_frames(&self) -> u64 {
            self.drain();
            self.queue.lock().unwrap().frames as u64
        }

        fn clear(&mut self) {
            self.drain();
            self.queue.lock().unwrap().frames = 0.0;
        }

        fn pause(&mut self) {
            self.drain();
            if !self.paused {
                self.paused = true;
                self.log.lock().unwrap().pause_count += 1;
            }
        }

        fn resume(&mut self) {
            self.drain();
            if self.paused {
                self.paused = false;
                self.log.lock().unwrap().resume_count += 1;
            }
        }

        fn fade_in(&self) {}

        fn fade_out_and_wait(&mut self) {}

        fn maintain(&mut self) -> Result<bool> {
            Ok(false)
        }

        fn failed(&self) -> bool {
            false
        }
    }

    impl Drop for MockOutput {
        fn drop(&mut self) {
            let mut log = self.log.lock().unwrap();
            log.active_outputs = log.active_outputs.saturating_sub(1);
            log.drop_count += 1;
        }
    }

    fn mock_opener(fixed_device_sr: Option<u32>, log: Arc<Mutex<MockAudioLog>>) -> OutputOpener {
        Arc::new(move |preferred, requested_sr, buffer_ms, volume_gain| {
            let device_sr = fixed_device_sr.unwrap_or(requested_sr);
            {
                let mut guard = log.lock().unwrap();
                guard
                    .opened_requests
                    .push((preferred.map(str::to_owned), requested_sr, buffer_ms));
                guard.active_outputs += 1;
            }
            Ok(Box::new(MockOutput {
                device_sr,
                volume_gain,
                log: Arc::clone(&log),
                paused: true,
                queue: Mutex::new(MockQueue {
                    frames: 0.0,
                    clock: Instant::now(),
                }),
            }))
        })
    }

    fn wait_for_state(
        player: &LocalFlacPlayer,
        timeout: Duration,
        pred: impl Fn(&LocalFlacState) -> bool,
    ) -> LocalFlacState {
        let deadline = Instant::now() + timeout;
        loop {
            let state = player.state();
            if pred(&state) {
                return state;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for expected state; last state: {state:?}"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    #[test]
    fn probe_and_decode_native_flac_rates_and_bit_depths() {
        let dir = temp_dir("probe");
        for (sr, bits, expected_label) in [
            (44_100, 16, "16-bit / 44.1 kHz FLAC"),
            (48_000, 24, "24-bit / 48 kHz FLAC"),
            (88_200, 24, "24-bit / 88.2 kHz FLAC"),
            (96_000, 24, "24-bit / 96 kHz FLAC"),
            (192_000, 24, "24-bit / 192 kHz FLAC"),
        ] {
            let frames = 2048;
            let path = write_test_flac(
                &dir,
                &format!("tone_{sr}_{bits}.flac"),
                sr,
                2,
                bits,
                frames,
                1_000.0,
            );
            let info = decode_flac_probe(&path).unwrap();
            assert_eq!(info.sample_rate_hz, sr);
            assert_eq!(info.channels, 2);
            assert_eq!(info.bit_depth, Some(bits));
            assert_eq!(info.total_frames, Some(frames as u64));
            assert_eq!(info.duration_ms, frames_to_ms(frames as u64, sr));
            assert_eq!(info.quality_label(), expected_label);

            let (dec_sr, dec_ch, samples) = decode_flac_samples(&path).unwrap();
            assert_eq!(dec_sr, sr);
            assert_eq!(dec_ch, 2);
            assert_eq!(samples.len(), frames * 2);
            let peak = samples.iter().fold(0.0f32, |m, &s| m.max(s.abs()));
            assert!(
                (peak - 0.25).abs() < 0.01,
                "{sr} Hz / {bits}-bit peak was {peak}"
            );
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn eq_shapes_both_tap_and_output_and_zero_volume_still_dances() {
        let dir = temp_dir("dsp");
        for track_sr in [44_100u32, 48_000, 96_000] {
            // Constant DC-like level (or 440 Hz tone) decoded from a real FLAC.
            let frames = 4096;
            let level_pcm = (0.15 * 8_388_607.0) as i32;
            let pcm = vec![level_pcm; frames * 2];
            let flac_bytes = build_verbatim_flac(track_sr, 2, 24, 512, &pcm);
            let path = dir.join(format!("dc_{track_sr}.flac"));
            std::fs::write(&path, flac_bytes).unwrap();

            let (sr, ch, decoded) = decode_flac_samples(&path).unwrap();
            assert_eq!(sr, track_sr);
            assert_eq!(ch, 2);

            let eq = crate::eq::shared();
            {
                let mut settings = eq.lock().unwrap();
                settings.on = true;
                settings.preamp_db = 6.0;
            }
            let tap = AudioTap::new();
            let mut pipeline = AudioPipeline::new(eq, Arc::clone(&tap), track_sr, track_sr);
            assert!(
                !pipeline.is_resampling_output(),
                "native device rate must not resample output"
            );

            let mut stereo_f64: Vec<f64> = decoded.iter().map(|&s| f64::from(s)).collect();
            let post_limiter = pipeline.process_stereo_f64(&mut stereo_f64);
            let expected = 0.15 * 10_f32.powf(6.0 / 20.0);

            // Tap received post-EQ signal at 44.1 kHz.
            let tapped = tap.window(128, 0);
            assert!(
                tapped.iter().all(|&v| (v - expected).abs() < 0.005),
                "track_sr={track_sr}: tap samples {:?} did not match post-EQ {expected}",
                &tapped[..4]
            );

            // Applying volume=0 (or mute) after the pipeline silences output
            // while leaving the visualizer tap completely untouched.
            let muted_out: Vec<f32> = post_limiter
                .iter()
                .map(|&s| s * volume_gain(0, false))
                .collect();
            assert!(muted_out.iter().all(|&v| v == 0.0));
            let tapped_after_mute = tap.window(128, 0);
            assert!(
                tapped_after_mute
                    .iter()
                    .all(|&v| (v - expected).abs() < 0.005),
                "zero volume must still feed the visualizers"
            );
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn output_resamples_only_when_device_rate_differs_from_track_rate() {
        let eq = crate::eq::shared();
        let tap = AudioTap::new();

        let native = AudioPipeline::new(eq.clone(), Arc::clone(&tap), 96_000, 96_000);
        assert!(!native.is_resampling_output());

        let mut resampled = AudioPipeline::new(eq, tap, 96_000, 48_000);
        assert!(resampled.is_resampling_output());

        let mut stereo = vec![0.25f64; 4_096 * 2];
        let out = resampled.process_stereo_f64(&mut stereo);
        // 96 kHz -> 48 kHz halves frame count (minus filter look-ahead warm-up).
        let out_frames = out.len() / 2;
        assert!(
            (out_frames as i64 - 2_048).abs() < 64,
            "expected ~2048 frames, got {out_frames}"
        );
    }

    #[test]
    fn seek_reaches_the_requested_position_accurately() {
        let dir = temp_dir("seek");
        // 2 seconds at 44.1 kHz = 88,200 frames -> round up to 88,576 (173 * 512).
        // First half is silence (0), second half is a +0.25 constant level.
        let sr = 44_100u32;
        let half_blocks = 88; // 45,056 frames (~1021 ms)
        let total_frames = half_blocks * 2 * 512;
        let mut pcm = vec![0i32; total_frames * 2];
        let loud = (0.25 * 32_767.0) as i32;
        for sample in &mut pcm[half_blocks * 512 * 2..] {
            *sample = loud;
        }
        let flac = build_verbatim_flac(sr, 2, 16, 512, &pcm);
        let path = dir.join("two_halves.flac");
        std::fs::write(&path, flac).unwrap();

        // Seeking past 1200 ms lands cleanly in the loud second half.
        let (dec_sr, dec_ch, tail) = decode_flac_samples_from(&path, 1_200).unwrap();
        assert_eq!(dec_sr, sr);
        assert_eq!(dec_ch, 2);
        let expected_frames = total_frames - (1_200 * sr as usize / 1_000);
        let got_frames = tail.len() / 2;
        assert!(
            (got_frames as i64 - expected_frames as i64).abs() <= 4,
            "seeked frames {got_frames} vs expected {expected_frames}"
        );
        assert!(
            tail.iter().all(|&s| (s - 0.25).abs() < 0.005),
            "every sample after 1200 ms should be in the loud half"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn controller_queue_pause_resume_seek_stop_and_zero_volume_tap() {
        let dir = temp_dir("controller");
        // Long enough (~3 seconds at 44.1 kHz) that we can pause/seek/next/prev
        // deterministically before EOF, even against a fast mock sink.
        let track1_path = write_test_flac(&dir, "one.flac", 44_100, 2, 16, 512 * 300, 440.0);
        let track2_path = write_test_flac(&dir, "two.flac", 96_000, 2, 24, 512 * 600, 880.0);

        let track1 = LocalFlacTrack {
            path: track1_path,
            title: "Track One".into(),
            artists: vec!["Artist A".into()],
            album: "Lossless Album".into(),
            duration_ms: 0, // populated from probe
            playlist_id: Some("pl-1".into()),
            track_id: Some("tr-1".into()),
            quality_label: None,
            sample_rate_hz: None,
            bit_depth: None,
        };
        let track2 = LocalFlacTrack {
            path: track2_path,
            title: "Track Two".into(),
            artists: vec!["Artist B".into()],
            album: "Lossless Album".into(),
            duration_ms: 0,
            playlist_id: Some("pl-1".into()),
            track_id: Some("tr-2".into()),
            quality_label: None,
            sample_rate_hz: None,
            bit_depth: None,
        };

        let eq = crate::eq::shared();
        let tap = AudioTap::new();
        let mock_log = Arc::new(Mutex::new(MockAudioLog::default()));
        let states = Arc::new(Mutex::new(Vec::<LocalFlacState>::new()));
        let states_cb = Arc::clone(&states);

        let player = LocalFlacPlayer::with_opener(
            eq,
            Arc::clone(&tap),
            Some("usb-dac".into()),
            100,
            Arc::new(move |st| {
                states_cb.lock().unwrap().push(st);
            }),
            mock_opener(None, Arc::clone(&mock_log)),
        );

        // Set volume to 0 BEFORE playing: output must be silent while AudioTap still dances!
        player.set_volume(0);
        player.play_track(track1.clone(), vec![track1.clone(), track2.clone()], 0);

        let playing = wait_for_state(&player, Duration::from_secs(3), |s| {
            s.playback == Playback::Playing
                && s.track.as_ref().is_some_and(|t| t.title == "Track One")
        });
        assert!(playing.active);
        assert_eq!(playing.queue_index, 0);
        assert_eq!(playing.volume_percent, 0);
        assert_eq!(playing.track.as_ref().unwrap().sample_rate_hz, Some(44_100));
        assert_eq!(playing.track.as_ref().unwrap().bit_depth, Some(16));
        assert_eq!(
            playing.track.as_ref().unwrap().quality_label.as_deref(),
            Some("16-bit / 44.1 kHz FLAC")
        );

        // Wait briefly for samples to flow through DSP & tap.
        let tap_deadline = Instant::now() + Duration::from_secs(2);
        while tap.window(128, 0).iter().all(|&v| v == 0.0) && Instant::now() < tap_deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            tap.window(256, 0).iter().any(|&v| v.abs() > 0.05),
            "AudioTap must receive non-zero post-EQ audio even when volume=0"
        );
        {
            let log = mock_log.lock().unwrap();
            assert!(!log.rendered_samples.is_empty());
            assert!(
                log.rendered_samples.iter().all(|&v| v == 0.0),
                "volume=0 must render silence to output"
            );
            assert_eq!(
                log.opened_requests[0],
                (Some("usb-dac".into()), 44_100, 100)
            );
        }

        // Pause and resume.
        player.pause();
        let paused = wait_for_state(&player, Duration::from_secs(2), |s| {
            s.playback == Playback::Paused
        });
        assert!(paused.active);

        // Seek while paused updates position without unpausing.
        player.seek(1_500);
        let seeked = wait_for_state(&player, Duration::from_secs(2), |s| {
            s.playback == Playback::Paused && s.position_ms >= 1_450 && s.position_ms <= 1_550
        });
        assert_eq!(seeked.playback, Playback::Paused);

        // Play/pause toggles back to Playing.
        player.play_pause();
        wait_for_state(&player, Duration::from_secs(2), |s| {
            s.playback == Playback::Playing
        });

        // Next advances to Track Two (96 kHz / 24-bit) and requests 96 kHz from output.
        player.set_volume(100);
        player.next();
        let second = wait_for_state(&player, Duration::from_secs(3), |s| {
            s.playback == Playback::Playing && s.queue_index == 1
        });
        assert_eq!(second.track.as_ref().unwrap().title, "Track Two");
        assert_eq!(second.track.as_ref().unwrap().sample_rate_hz, Some(96_000));
        assert_eq!(second.track.as_ref().unwrap().bit_depth, Some(24));
        {
            let log = mock_log.lock().unwrap();
            assert!(
                log.opened_requests.iter().any(|(_, sr, _)| *sr == 96_000),
                "expected 96 kHz output open request: {:?}",
                log.opened_requests
            );
        }

        // Pause immediately while position < 3s, then previous() steps back to index 0.
        player.pause();
        wait_for_state(&player, Duration::from_secs(2), |s| {
            s.playback == Playback::Paused
        });
        player.seek(500);
        wait_for_state(&player, Duration::from_secs(2), |s| s.position_ms <= 600);
        player.previous();
        let back = wait_for_state(&player, Duration::from_secs(3), |s| {
            s.playback == Playback::Playing && s.queue_index == 0
        });
        assert_eq!(back.track.as_ref().unwrap().title, "Track One");

        // Switch audio device cleanly.
        player.set_audio_device(Some("headphones".into()), 80);
        let switch_deadline = Instant::now() + Duration::from_secs(2);
        while !mock_log
            .lock()
            .unwrap()
            .opened_requests
            .iter()
            .any(|(dev, _, buf)| dev.as_deref() == Some("headphones") && *buf == 80)
            && Instant::now() < switch_deadline
        {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            mock_log
                .lock()
                .unwrap()
                .opened_requests
                .iter()
                .any(|(dev, _, buf)| dev.as_deref() == Some("headphones") && *buf == 80)
        );

        // Stop releases the audio output stream completely.
        player.stop();
        let stopped = wait_for_state(&player, Duration::from_secs(2), |s| {
            !s.active && s.playback == Playback::Stopped
        });
        assert_eq!(stopped.position_ms, 0);
        assert_eq!(stopped.error, None);
        assert_eq!(
            mock_log.lock().unwrap().active_outputs,
            0,
            "stop() must release the OS audio stream"
        );
        assert!(
            tap.window(64, 0).iter().all(|&v| v == 0.0),
            "stop() must clear the visualizer tap"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn output_options_request_native_rate_and_clamp_buffer() {
        let dev = fastframe_audio::Device::Id(DeviceId::new("dac"));
        let opts = output_options(dev.clone(), 192_000, 10_000);
        assert_eq!(opts.device, dev);
        assert_eq!(opts.sample_rate, Some(192_000));
        assert_eq!(opts.channels, 2);
        assert!(opts.follow_default);
        assert_eq!(
            opts.buffer,
            Buffer::FixedOnWindows(BufferSize::Duration(Duration::from_millis(u64::from(
                *crate::sink::BUFFER_MS_RANGE.end()
            ))))
        );
    }

    #[test]
    fn muted_silences_output_while_visualizer_tap_stays_active() {
        let dir = temp_dir("muted");
        let path = write_test_flac(&dir, "tone.flac", 48_000, 2, 24, 512 * 300, 440.0);
        let track = LocalFlacTrack {
            path,
            title: "Muted Track".into(),
            ..Default::default()
        };
        let tap = AudioTap::new();
        let mock_log = Arc::new(Mutex::new(MockAudioLog::default()));
        let player = LocalFlacPlayer::with_opener(
            crate::eq::shared(),
            Arc::clone(&tap),
            None,
            100,
            Arc::new(|_| {}),
            mock_opener(Some(48_000), Arc::clone(&mock_log)),
        );

        player.set_volume(100);
        player.set_muted(true);
        player.play_track(track, Vec::new(), 0);

        let playing = wait_for_state(&player, Duration::from_secs(2), |s| {
            s.playback == Playback::Playing
        });
        assert!(playing.muted);
        assert_eq!(playing.volume_percent, 100);

        let deadline = Instant::now() + Duration::from_secs(2);
        while tap.window(128, 0).iter().all(|&v| v == 0.0) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            tap.window(256, 0).iter().any(|&v| v.abs() > 0.05),
            "muted playback must still feed post-EQ audio to AudioTap"
        );
        let log = mock_log.lock().unwrap();
        assert!(!log.rendered_samples.is_empty());
        assert!(log.rendered_samples.iter().all(|&v| v == 0.0));

        player.stop();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn mono_flac_expands_cleanly_to_stereo_pipeline_and_tap() {
        let dir = temp_dir("mono");
        let path = write_test_flac(&dir, "mono.flac", 48_000, 1, 16, 4096, 1_000.0);
        let info = decode_flac_probe(&path).unwrap();
        assert_eq!(info.channels, 1);
        assert_eq!(info.sample_rate_hz, 48_000);
        assert_eq!(info.bit_depth, Some(16));

        let (sr, ch, samples) = decode_flac_samples(&path).unwrap();
        assert_eq!(sr, 48_000);
        assert_eq!(ch, 1);
        assert_eq!(samples.len(), 4096);

        let mut stereo = Vec::new();
        let f64_mono: Vec<f64> = samples.iter().map(|&s| f64::from(s)).collect();
        to_interleaved_stereo_f64(&f64_mono, 1, &mut stereo);
        assert_eq!(stereo.len(), 4096 * 2);
        assert!(stereo.as_chunks::<2>().0.iter().all(|f| f[0] == f[1]));

        let tap = AudioTap::new();
        let mut pipeline =
            AudioPipeline::new(crate::eq::shared(), Arc::clone(&tap), 48_000, 48_000);
        let out = pipeline.process_stereo_f64(&mut stereo);
        assert_eq!(out.len(), 4096 * 2);
        assert!(tap.window(256, 0).iter().any(|&v| v.abs() > 0.05));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn previous_restarts_past_three_seconds_and_steps_back_before_three_seconds() {
        let dir = temp_dir("previous");
        // ~5 seconds at 44.1 kHz (440 * 512 = 225,280 frames = 5108 ms).
        let p1 = write_test_flac(&dir, "a.flac", 44_100, 2, 16, 512 * 440, 440.0);
        let p2 = write_test_flac(&dir, "b.flac", 44_100, 2, 16, 512 * 440, 660.0);
        let t1 = LocalFlacTrack {
            path: p1,
            title: "First".into(),
            ..Default::default()
        };
        let t2 = LocalFlacTrack {
            path: p2,
            title: "Second".into(),
            ..Default::default()
        };

        let mock_log = Arc::new(Mutex::new(MockAudioLog::default()));
        let player = LocalFlacPlayer::with_opener(
            crate::eq::shared(),
            AudioTap::new(),
            None,
            100,
            Arc::new(|_| {}),
            mock_opener(None, mock_log),
        );

        player.play_track(t2.clone(), vec![t1, t2], 1);
        wait_for_state(&player, Duration::from_secs(2), |s| {
            s.playback == Playback::Playing && s.queue_index == 1
        });
        player.pause();
        wait_for_state(&player, Duration::from_secs(2), |s| {
            s.playback == Playback::Paused
        });

        // Seek past 3 seconds -> previous() restarts current track at 0 ms.
        player.seek(4_000);
        wait_for_state(&player, Duration::from_secs(2), |s| s.position_ms >= 3_950);
        player.previous();
        let restarted = wait_for_state(&player, Duration::from_secs(2), |s| s.position_ms < 100);
        assert_eq!(restarted.queue_index, 1);

        // Now at <3 seconds -> previous() steps back to track index 0.
        player.previous();
        let stepped = wait_for_state(&player, Duration::from_secs(2), |s| {
            s.queue_index == 0 && s.playback == Playback::Playing
        });
        assert_eq!(stepped.track.as_ref().unwrap().title, "First");

        player.stop();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn end_of_track_auto_advances_queue_and_stops_cleanly_at_end() {
        let dir = temp_dir("eof");
        // Short 2-block (~23 ms) tracks finish immediately on the mock output.
        let p1 = write_test_flac(&dir, "short1.flac", 44_100, 2, 16, 1024, 440.0);
        let p2 = write_test_flac(&dir, "short2.flac", 44_100, 2, 16, 1024, 880.0);
        let t1 = LocalFlacTrack {
            path: p1,
            title: "Short 1".into(),
            ..Default::default()
        };
        let t2 = LocalFlacTrack {
            path: p2,
            title: "Short 2".into(),
            ..Default::default()
        };

        let seen_titles = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen_cb = Arc::clone(&seen_titles);
        let mock_log = Arc::new(Mutex::new(MockAudioLog::default()));
        let player = LocalFlacPlayer::with_opener(
            crate::eq::shared(),
            AudioTap::new(),
            None,
            100,
            Arc::new(move |st| {
                if st.playback == Playback::Playing
                    && let Some(track) = &st.track
                {
                    let mut titles = seen_cb.lock().unwrap();
                    if titles.last() != Some(&track.title) {
                        titles.push(track.title.clone());
                    }
                }
            }),
            mock_opener(None, Arc::clone(&mock_log)),
        );

        player.play_track(t1.clone(), vec![t1, t2], 0);
        let finished = wait_for_state(&player, Duration::from_secs(3), |s| {
            !s.active && s.playback == Playback::Stopped && s.queue_index == 1
        });
        assert_eq!(finished.error, None);
        assert_eq!(
            *seen_titles.lock().unwrap(),
            vec!["Short 1".to_string(), "Short 2".to_string()]
        );
        assert_eq!(mock_log.lock().unwrap().active_outputs, 0);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn missing_file_reports_error_and_releases_output() {
        let dir = temp_dir("missing");
        let track = LocalFlacTrack {
            path: dir.join("does_not_exist.flac"),
            title: "Ghost".into(),
            ..Default::default()
        };
        let mock_log = Arc::new(Mutex::new(MockAudioLog::default()));
        let player = LocalFlacPlayer::with_opener(
            crate::eq::shared(),
            AudioTap::new(),
            None,
            100,
            Arc::new(|_| {}),
            mock_opener(None, Arc::clone(&mock_log)),
        );

        player.play_track(track, Vec::new(), 0);
        let failed = wait_for_state(&player, Duration::from_secs(2), |s| {
            !s.active && s.playback == Playback::Stopped && s.error.is_some()
        });
        assert!(failed.error.as_deref().unwrap().contains("failed to open"));
        assert_eq!(mock_log.lock().unwrap().active_outputs, 0);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn local_render_ramps_volume_smoothly_and_adapts_channel_counts() {
        let queue = SampleQueue::new(48_000);
        let volume = Arc::new(AtomicU32::new(1.0f32.to_bits()));
        let transport = Envelope::closed(48_000);
        transport.level.store(ENVELOPE_SCALE, Ordering::Relaxed);
        transport.target.store(ENVELOPE_SCALE, Ordering::Relaxed);

        let mut render = LocalRender::new(
            Arc::clone(&queue),
            Arc::clone(&volume),
            Arc::clone(&transport),
        );
        render.configure(48_000, 2);

        // Stereo volume ramp down from 1.0 to 0.0 over 30 ms (1440 frames at 48 kHz).
        // A 0.5 level sits below the limiter's knee, so only the gain shows. The
        // limiter's look-ahead delays every frame by LOOKAHEAD frames.
        queue.push_stereo(&vec![0.5; 2_000 * 2]);
        volume.store(0.0f32.to_bits(), Ordering::Relaxed);
        let mut stereo_out = vec![0.0; (LOOKAHEAD + 1_500) * 2];
        render.render(&mut stereo_out);
        assert!(stereo_out[..LOOKAHEAD * 2].iter().all(|&s| s == 0.0));
        let signal = &stereo_out[LOOKAHEAD * 2..];
        assert!((signal[0] - 0.5).abs() < 1e-6);
        assert!((signal[1] - 0.5).abs() < 1e-6);
        assert_eq!(signal[1_440 * 2], 0.0);
        assert_eq!(signal[1_440 * 2 + 1], 0.0);
        let left: Vec<f32> = signal.iter().step_by(2).take(1_440).copied().collect();
        assert!(left.windows(2).all(|w| w[0] >= w[1]));

        // Mono device downmixes the limited stereo pair to its average.
        queue.clear();
        volume.store(1.0f32.to_bits(), Ordering::Relaxed);
        render.ramp.current = 1.0;
        render.ramp.target = 1.0;
        render.ramp.remaining = 0;
        render.configure(48_000, 1);
        queue.push_stereo(&[0.4, 0.2].repeat(4));
        let mut mono_out = vec![0.0; LOOKAHEAD + 4];
        render.render(&mut mono_out);
        assert!(mono_out[..LOOKAHEAD].iter().all(|&s| s == 0.0));
        for &sample in &mono_out[LOOKAHEAD..LOOKAHEAD + 4] {
            assert!((sample - 0.3).abs() < 1e-6);
        }
        assert!(mono_out[LOOKAHEAD + 4..].iter().all(|&s| s == 0.0));

        // 4-channel device routes stereo into front L/R and zeros rear channels.
        render.configure(48_000, 4);
        queue.push_stereo(&[0.6, -0.3]);
        let mut quad_out = vec![0.0; (LOOKAHEAD + 1) * 4];
        render.render(&mut quad_out);
        let frame = &quad_out[LOOKAHEAD * 4..];
        assert!((frame[0] - 0.6).abs() < 1e-6);
        assert!((frame[1] + 0.3).abs() < 1e-6);
        assert_eq!(frame[2], 0.0);
        assert_eq!(frame[3], 0.0);
    }

    /// The limiter's look-ahead at 48 kHz: 8 ms.
    const LOOKAHEAD: usize = 384;

    /// A stereo output at a device rate, opened with a volume gain.
    fn open_render(sample_rate: u32, gain: f32) -> (Arc<SampleQueue>, LocalRender) {
        let queue = SampleQueue::new(sample_rate);
        let volume = Arc::new(AtomicU32::new(gain.to_bits()));
        let transport = Envelope::closed(sample_rate);
        transport.level.store(ENVELOPE_SCALE, Ordering::Relaxed);
        transport.target.store(ENVELOPE_SCALE, Ordering::Relaxed);
        let mut render = LocalRender::new(Arc::clone(&queue), volume, transport);
        render.configure(sample_rate, 2);
        (queue, render)
    }

    #[test]
    fn surround_downmix_folds_centre_and_rear_at_minus_3_db_and_drops_lfe() {
        let minus_3_db = DOWNMIX_MINUS_3_DB;
        assert!((minus_3_db - 0.707).abs() < 1e-3, "-3 dB is about 0.707");

        // A centre-only 5.1 frame lands equally in both outputs; the LFE is gone.
        let (left, right) = fold_frame_to_stereo(&[0.0, 0.0, 1.0, 0.9, 0.0, 0.0]);
        assert!((left - minus_3_db).abs() < 1e-12);
        assert!((right - minus_3_db).abs() < 1e-12);

        // A rear-left-only frame reaches the left output only, at -3 dB.
        let (left, right) = fold_frame_to_stereo(&[0.0, 0.0, 0.0, 0.0, 1.0, 0.0]);
        assert!((left - minus_3_db).abs() < 1e-12);
        assert!(right.abs() < 1e-12);

        // Front channels pass through unchanged.
        let (left, right) = fold_frame_to_stereo(&[0.3, -0.2, 0.0, 0.0, 0.0, 0.0]);
        assert_eq!((left, right), (0.3, -0.2));

        // Quad (front L/R, rear L/R): the rear pair folds in at -3 dB.
        let (left, right) = fold_frame_to_stereo(&[0.0, 0.0, 1.0, 1.0]);
        assert!((left - minus_3_db).abs() < 1e-12);
        assert!((right - minus_3_db).abs() < 1e-12);

        // The interleaved converter applies the same fold frame by frame.
        let mut out = Vec::new();
        to_interleaved_stereo_f64(
            &[0.0, 0.0, 1.0, 0.9, 0.0, 0.0, 0.3, -0.2, 0.0, 0.0, 0.0, 0.0],
            6,
            &mut out,
        );
        assert_eq!(out.len(), 4);
        assert!((out[0] - minus_3_db).abs() < 1e-12);
        assert!((out[1] - minus_3_db).abs() < 1e-12);
        assert_eq!((out[2], out[3]), (0.3, -0.2));
    }

    #[test]
    fn quiet_playback_is_not_compressed_by_the_output_limiter() {
        // A loud decoded signal (+12 dB after EQ) at 25% volume is 0.0625 at the
        // device: far below full scale, so it must come out untouched. A limiter
        // ahead of the volume would have squashed the 4.0 first.
        let gain = volume_gain(25, false);
        let (queue, mut render) = open_render(48_000, gain);
        queue.push_stereo(&vec![4.0; 1_000 * 2]);
        let mut out = vec![0.0f32; (LOOKAHEAD + 1_000) * 2];
        render.render(&mut out);
        let steady = &out[(LOOKAHEAD + 100) * 2..(LOOKAHEAD + 900) * 2];
        assert!(
            steady.iter().all(|&s| (s - 4.0 * gain).abs() < 1e-6),
            "quiet output must be exactly the volume-scaled input"
        );
    }

    #[test]
    fn the_output_limiter_holds_loud_output_below_full_scale() {
        // At full volume the signal reaches the speakers, so it is limited.
        let (queue, mut render) = open_render(48_000, volume_gain(100, false));
        queue.push_stereo(&vec![4.0; 1_000 * 2]);
        let mut out = vec![0.0f32; (LOOKAHEAD + 1_000) * 2];
        render.render(&mut out);
        let peak = out.iter().fold(0.0f32, |m, &s| m.max(s.abs()));
        assert!(peak <= 1.0, "nothing may leave above full scale: {peak}");
        assert!(peak > 0.85, "and it really was limited: {peak}");
    }

    #[test]
    fn the_visualiser_tap_is_identical_whatever_the_volume() {
        let input: Vec<f64> = (0..4_096)
            .flat_map(|i| {
                let s = 0.5 * (i as f64 * 0.05).sin();
                [s, s]
            })
            .collect();
        let tap_after_render = |volume_percent: u8| {
            let tap = AudioTap::new();
            let mut pipeline =
                AudioPipeline::new(crate::eq::shared(), Arc::clone(&tap), 44_100, 44_100);
            let mut stereo = input.clone();
            let processed = pipeline.process_stereo_f64(&mut stereo);
            let (queue, mut render) = open_render(44_100, volume_gain(volume_percent, false));
            queue.push_stereo(&processed);
            let mut out = vec![0.0f32; processed.len()];
            render.render(&mut out);
            (tap.window(256, 0), out)
        };
        let (tap_silent, out_silent) = tap_after_render(0);
        let (tap_loud, out_loud) = tap_after_render(100);
        assert_eq!(tap_silent, tap_loud, "volume must not move the visualiser");
        assert!(tap_loud.iter().any(|&s| s.abs() > 0.1));
        assert!(out_silent.iter().all(|&s| s == 0.0));
        assert!(out_loud.iter().any(|&s| s.abs() > 0.1));
    }

    /// A worker already playing `track` on a mock device, driven step by step
    /// from the test thread instead of through its command loop.
    fn mock_worker(log: Arc<Mutex<MockAudioLog>>, track: LocalFlacTrack) -> PlayerWorker {
        let mut worker = PlayerWorker::new(
            crate::eq::shared(),
            AudioTap::new(),
            None,
            100,
            Arc::new(AtomicU32::new(1.0f32.to_bits())),
            Arc::new(Mutex::new(LocalFlacState::default())),
            Arc::new(|_: LocalFlacState| {}),
            mock_opener(None, log),
        );
        worker.handle_command(Command::PlayTrack {
            track: Box::new(track.clone()),
            queue: vec![track],
            start_index: 0,
        });
        worker
    }

    #[test]
    fn seek_past_the_end_of_the_file_is_clamped_to_the_file_length() {
        let dir = temp_dir("seek-clamp");
        // About one second of audio, but the library says ten.
        let path = write_test_flac(&dir, "short.flac", 44_100, 2, 16, 512 * 86, 440.0);
        let track = LocalFlacTrack {
            path,
            duration_ms: 10_000,
            ..LocalFlacTrack::default()
        };
        let log = Arc::new(Mutex::new(MockAudioLog::default()));
        let mut worker = mock_worker(log, track);
        let file_ms = frames_to_ms(512 * 86, 44_100);
        assert_eq!(worker.state.duration_ms, file_ms);

        worker.seek_to(9_000);
        assert!(
            worker.state.position_ms <= file_ms,
            "seek landed at {} ms, past the {file_ms} ms file",
            worker.state.position_ms
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn switching_device_mid_track_continues_from_what_was_heard() {
        let dir = temp_dir("device-switch");
        let path = write_test_flac(&dir, "switch.flac", 44_100, 2, 16, 512 * 300, 440.0);
        let track = LocalFlacTrack {
            path,
            ..LocalFlacTrack::default()
        };
        let log = Arc::new(Mutex::new(MockAudioLog::default()));
        let mut worker = mock_worker(Arc::clone(&log), track);

        // Decode ahead until the device queue is full.
        for _ in 0..1_000 {
            if worker
                .output
                .as_ref()
                .is_some_and(|out| out.queued_frames() >= 8_000)
            {
                break;
            }
            worker.step_playback();
        }
        let queued = worker.output.as_ref().unwrap().queued_frames();
        assert!(queued >= 8_000, "the mock device must hold real frames");
        let heard_before = worker.current_position_ms();
        assert!(heard_before < 50, "almost nothing is heard yet");

        worker.handle_command(Command::SetAudioDevice {
            device: Some("headphones".into()),
            buffer_ms: 80,
        });
        assert_eq!(log.lock().unwrap().opened_requests.len(), 2);
        let heard_after = worker.current_position_ms();
        assert!(
            heard_after <= heard_before + 50,
            "the new device must not skip the {queued} queued frames: {heard_after} ms"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_track_ends_with_silence_that_lets_the_limiter_tail_play_out() {
        let dir = temp_dir("eof-tail");
        let path = write_test_flac(&dir, "tail.flac", 44_100, 2, 16, 512 * 20, 440.0);
        let track = LocalFlacTrack {
            path,
            ..LocalFlacTrack::default()
        };
        let log = Arc::new(Mutex::new(MockAudioLog::default()));
        let mut worker = mock_worker(Arc::clone(&log), track);

        for _ in 0..100_000 {
            if worker.stream.as_ref().is_some_and(|s| s.eof) {
                break;
            }
            if worker.step_playback() {
                thread::sleep(Duration::from_millis(1));
            }
        }
        assert!(worker.stream.as_ref().is_some_and(|s| s.eof));

        let log = log.lock().unwrap();
        let rendered = &log.rendered_samples;
        let tail = 2 * (44_100 / 100 + 1);
        assert!(
            rendered[rendered.len() - tail..].iter().all(|&s| s == 0.0),
            "the track must end with silence after its last sample"
        );
        assert!(
            rendered[rendered.len() - tail - 882..rendered.len() - tail]
                .iter()
                .any(|&s| s.abs() > 1e-3),
            "the audio itself comes before that silence"
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
