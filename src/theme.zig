//! Every colour the screen uses, in one place. The palette is Tokyo Night,
//! chosen because it reads well on the dark terminals this is used in and
//! because its six semantic slots — text, dim, accent, ok, warn, err — are
//! the six a download list needs. Changing the theme is changing this
//! file; nothing in `tui.zig` names an RGB.

const vaxis = @import("vaxis");

pub const Style = vaxis.Cell.Style;
pub const Color = vaxis.Cell.Color;

fn rgb(hex: u24) Color {
    return .{ .rgb = .{ @intCast(hex >> 16), @intCast((hex >> 8) & 0xff), @intCast(hex & 0xff) } };
}

pub const fg: Color = rgb(0xc0caf5);
pub const dim: Color = rgb(0x565f89);
pub const dimmer: Color = rgb(0x3b4261);
pub const accent: Color = rgb(0x7aa2f7);
pub const accent2: Color = rgb(0xbb9af7);
pub const ok: Color = rgb(0x9ece6a);
pub const warn: Color = rgb(0xe0af68);
pub const err: Color = rgb(0xf7768e);
pub const cyan: Color = rgb(0x7dcfff);
pub const bg_alt: Color = rgb(0x1f2335);

pub const text: Style = .{ .fg = fg };
pub const muted: Style = .{ .fg = dim };
pub const faint: Style = .{ .fg = dimmer };
pub const title: Style = .{ .fg = accent, .bold = true };
pub const strong: Style = .{ .fg = fg, .bold = true };
pub const border: Style = .{ .fg = dimmer };
pub const border_focus: Style = .{ .fg = accent };
pub const selected: Style = .{ .fg = fg, .bg = bg_alt };
pub const key: Style = .{ .fg = accent, .bold = true };
pub const tab_on: Style = .{ .fg = accent, .bold = true, .ul_style = .single, .ul = accent };
pub const tab_off: Style = .{ .fg = dim };

pub const running: Style = .{ .fg = accent };
pub const done: Style = .{ .fg = ok };
pub const failed: Style = .{ .fg = err };
pub const paused: Style = .{ .fg = warn };
pub const queued: Style = .{ .fg = dim };

pub const bar_fill: Style = .{ .fg = accent };
pub const bar_done: Style = .{ .fg = ok };
pub const bar_rest: Style = .{ .fg = dimmer };
pub const spark: Style = .{ .fg = accent2 };
